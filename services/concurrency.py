"""Concurrency service (Milestone 6): mutual exclusion and deadlock management across nodes.

Every node hosts one of these. What it does depends on the node's role:

  LOCK HOST   (core, cloud, managers)  owns some shared resources and grants them one holder at a
                                        time, FIFO.  Resource -> host map: cfg["lock_resources"],
                                        default crew@core, fuel_truck@cloud, link_slot@mgr1,
                                        site_controller@mgr2.
  TASK RUNNER (sites)                  runs dispatch tasks.  A task needs 2-3 resources and holds
                                        each one until it has all of them (hold-and-wait), so
                                        circular wait, and therefore deadlock, is possible.
  DETECTOR    (the elected leader)     every `detect_interval` s asks every lock host for its
                                        wait-for edges, merges them into ONE global wait-for graph,
                                        finds cycles and aborts one victim per cycle.  The detector is
                                        whoever Bully election (Milestone 4) made leader, so if the
                                        leader dies the next one takes over detection.

Resource-allocation graph:  P -> R  = process waits for R        R -> P = R is held by P
Wait-for graph (derived):   P -> Q  = P waits for a resource Q holds.  A cycle = deadlock.

Strategies (cfg["deadlock_strategy"], switchable at run time with CC_CONFIG):
  none             no handling: a deadlock lasts forever
  prevent          resource ordering: tasks acquire in one global order -> circular wait impossible
  detect_random    detection + recovery, RANDOM victim                          (baseline)
  detect_priority  detection + recovery, POWER-AWARE victim: abort the lowest-priority task in the
                   cycle (routine alarms before power-failure alarms); every earlier abort raises a
                   task's effective priority (aging) so nothing starves        (proposed)

Message types: LOCK_ACQUIRE LOCK_RELEASE TASK_ABORT WFG_QUERY TASK_START TASK_DONE CC_CONFIG CC_STATS
               SHARED_READ SHARED_WRITE RACE_RUN RACE_DONE
"""
import asyncio
import random
import time
from collections import deque

from kernel.election import _num

CC_TYPES = {"LOCK_ACQUIRE", "LOCK_RELEASE", "TASK_ABORT", "WFG_QUERY", "TASK_START", "TASK_DONE",
            "CC_CONFIG", "CC_STATS", "SHARED_READ", "SHARED_WRITE", "RACE_RUN", "RACE_DONE"}
RANK = ["site_controller", "crew", "link_slot", "fuel_truck"]      # global acquisition order


def find_cycles(graph):
    """graph: waiter -> holder.  Out-degree is 1 (a task waits for one resource at a time), so
    cycles are disjoint and following the pointers finds them all."""
    cycles, seen = [], set()
    for s in graph:
        path, pos, node = [], {}, s
        while True:
            if node in pos:
                cycles.append(path[pos[node]:])
                break
            if node in seen or node not in graph:
                break
            pos[node] = len(path)
            path.append(node)
            node = graph[node]
        seen.update(path)
    return cycles


def rag_to_dot(edges, cycle=(), title="RAG"):
    """edges: list of (src, dst, kind) with kind 'request' (P->R) or 'assigned' (R->P)."""
    cyc = set(cycle)
    names = {x for e in edges for x in e[:2]}
    out = [f'digraph "{title}" {{', "  rankdir=LR;"]
    for n in sorted(names):
        shape = "circle" if n.startswith("t") and "-" in n else "box"
        out.append(f'  "{n}" [shape={shape}{" color=red penwidth=2" if n in cyc else ""}];')
    for a, b, kind in edges:
        out.append(f'  "{a}" -> "{b}" [style={"dashed" if kind == "request" else "solid"} label="{kind}"];')
    return "\n".join(out + ["}"])


class ConcurrencyService:
    def __init__(self, node):
        self.node = node
        cfg = node.cfg
        self.strategy = cfg.get("deadlock_strategy", "none")      # default off: earlier milestones unchanged
        self.interval = cfg.get("detect_interval", 0.5)
        self.lock_timeout = cfg.get("lock_timeout", 30.0)
        self.rng = random.Random(cfg.get("seed", 0) + 99 + _num(node.name))
        self.gen = 0                                              # run generation, bumped by CC_CONFIG
        self.locks = {}            # host: res -> {"holder": task|None, "queue": deque of waiters}
        self.values = {}           # core: shared counters used by the race experiment
        self.tasks = []            # site: running asyncio tasks
        self.done, self.race_done = {}, {}                        # client: results
        self.grants = self.waits = self.aborted_waits = 0         # host counters
        self.det_rounds = self.det_msgs = self.deadlocks = self.victims = self.false_aborts = 0
        self.task_aborts, self.wasted = 0, 0.0                    # site: aborts suffered, work thrown away
        self.last_cycles = []                                     # detector: last cycles found
        nodes = cfg["nodes"]
        self._core = node.core_name
        self._cloud = next((x["name"] for x in nodes if x["role"] == "cloud"), self._core)
        self._mgrs = sorted((x["name"] for x in nodes if x["role"] == "manager"), key=_num)

    # ------------------------------------------------------------- naming
    def host_of(self, res):
        m = self.node.cfg.get("lock_resources", {})
        if res in m:
            return m[res]
        default = {"crew": self._core, "fuel_truck": self._cloud, "counter": self._core,
                   "link_slot": self._mgrs[0] if self._mgrs else self._core,
                   "site_controller": self._mgrs[1] if len(self._mgrs) > 1 else self._core}
        return default.get(res, self._core)

    def lock_hosts(self):
        return sorted({self.host_of(r) for r in RANK})

    def start(self):
        if self.node.role == "manager":
            asyncio.create_task(self._guard(self._detector()))

    async def _guard(self, coro):
        try:
            await coro
        except Exception as e:                                    # keep background failures visible
            self.node.log.event("cc_task_error", error=repr(e))

    # ------------------------------------------------------ message entry
    async def handle(self, m):
        n, t, p = self.node, m.type, m.payload
        if t == "LOCK_ACQUIRE":
            return await self._acquire(m)
        if t == "LOCK_RELEASE":
            self._release(p["task"])
            return n.msg(m.sender, "ACK")
        if t == "TASK_ABORT":
            return n.msg(m.sender, "ACK", {"aborted": self._abort_wait(p["task"], p["res"])})
        if t == "WFG_QUERY":
            return n.msg(m.sender, "WFG_REPLY", {"edges": self._edges(), "holders": self._holders()})
        if t == "TASK_START":
            self.tasks.append(asyncio.create_task(self._guard(self._run_task(p, m.sender))))
            return n.msg(m.sender, "ACK")
        if t == "TASK_DONE":
            self.done[p["id"]] = p
        elif t == "RACE_DONE":
            self.race_done[m.sender] = p
        elif t == "CC_CONFIG":
            self._configure(p)
            return n.msg(m.sender, "ACK")
        elif t == "CC_STATS":
            return n.msg(m.sender, "CC_STATS", {"node": n.name, **self.stats()})
        elif t == "SHARED_READ":
            return n.msg(m.sender, "SHARED_VALUE", {"value": self.values.get(p["key"], 0)})
        elif t == "SHARED_WRITE":
            self.values[p["key"]] = p["value"]
            return n.msg(m.sender, "ACK")
        elif t == "RACE_RUN":
            self.tasks.append(asyncio.create_task(self._guard(self._race(p, m.sender))))
            return n.msg(m.sender, "ACK")
        return None

    def _configure(self, p):
        """New run: set strategy/interval, wipe all lock state and stop old tasks."""
        self.strategy = p.get("strategy", self.strategy)
        self.interval = p.get("interval", self.interval)
        self.gen = p.get("gen", self.gen + 1)
        for L in self.locks.values():
            for e in L["queue"]:
                if not e["fut"].done():
                    e["fut"].set_result("ABORT")
        self.locks.clear()
        self.values.clear()
        for t in self.tasks:
            t.cancel()
        self.tasks = []
        self.done, self.race_done = {}, {}
        self.grants = self.waits = self.aborted_waits = 0
        self.det_rounds = self.det_msgs = self.deadlocks = self.victims = self.false_aborts = 0
        self.task_aborts, self.wasted = 0, 0.0
        self.last_cycles = []
        self.node.log.event("cc_configured", strategy=self.strategy, interval=self.interval, gen=self.gen)

    # --------------------------------------------------------- lock host
    async def _acquire(self, m):
        n, p = self.node, m.payload
        res, task = p["res"], p["task"]
        if p.get("gen") != self.gen:                              # leftover from a previous run
            return n.msg(m.sender, "LOCK_ABORTED", {"res": res, "task": task})
        L = self.locks.setdefault(res, {"holder": None, "queue": deque()})
        if L["holder"] in (None, task):
            L["holder"] = task
            self.grants += 1
            return n.msg(m.sender, "LOCK_GRANTED", {"res": res, "task": task})
        fut = asyncio.get_running_loop().create_future()
        L["queue"].append({"task": task, "prio": p.get("prio", 1), "aborts": p.get("aborts", 0),
                           "since": time.time(), "fut": fut})
        self.waits += 1
        outcome = await fut                                       # "GRANT" or "ABORT"
        return n.msg(m.sender, "LOCK_GRANTED" if outcome == "GRANT" else "LOCK_ABORTED",
                     {"res": res, "task": task})

    @staticmethod
    def _pass(L):
        """Give the lock to the next live waiter, or free it."""
        while L["queue"]:
            e = L["queue"].popleft()
            if not e["fut"].done():
                L["holder"] = e["task"]
                e["fut"].set_result("GRANT")
                return
        L["holder"] = None

    def _release(self, task):
        """Drop everything `task` holds or waits for on this host."""
        for L in self.locks.values():
            for e in list(L["queue"]):
                if e["task"] == task:
                    L["queue"].remove(e)
                    if not e["fut"].done():
                        e["fut"].set_result("ABORT")
            if L["holder"] == task:
                self._pass(L)

    def _abort_wait(self, task, res):
        """Detector's victim order. Only cancels the wait if the task is STILL waiting here
        (otherwise the deadlock was already gone and aborting would be a false abort)."""
        L = self.locks.get(res)
        for e in (list(L["queue"]) if L else []):
            if e["task"] == task and not e["fut"].done():
                L["queue"].remove(e)
                e["fut"].set_result("ABORT")
                self.aborted_waits += 1
                return True
        return False

    def _edges(self):
        return [{"waiter": e["task"], "holder": L["holder"], "res": res, "prio": e["prio"],
                 "aborts": e["aborts"], "since": e["since"]}
                for res, L in self.locks.items() if L["holder"] is not None
                for e in L["queue"] if not e["fut"].done()]

    def _holders(self):
        return {res: L["holder"] for res, L in self.locks.items() if L["holder"] is not None}

    # ----------------------------------------------------------- detector
    async def _detector(self):
        await asyncio.sleep(1.0)
        while True:
            await asyncio.sleep(self.interval)
            el = self.node.election
            if self.strategy.startswith("detect") and el and el.leader == self.node.name:
                await self._detect_round()

    async def _detect_round(self):
        n = self.node
        hosts = self.lock_hosts()
        replies = await asyncio.gather(*[n.send(h, n.msg(h, "WFG_QUERY"), wait_reply=True,
                                                emulate=False, timeout=1.0) for h in hosts])
        self.det_rounds += 1
        self.det_msgs += len(hosts) + sum(r is not None for r in replies)
        graph, info = {}, {}
        for r in replies:
            if r is not None:
                for e in r.payload["edges"]:
                    graph[e["waiter"]], info[e["waiter"]] = e["holder"], e
        for cyc in find_cycles(graph):
            victim = self._victim(cyc, info)
            self.deadlocks += 1
            self.last_cycles.append(cyc)
            n.lc += 1
            n.log.event("deadlock_detected", cycle=cyc, victim=victim, strategy=self.strategy, lc=n.lc,
                        waits_on={t: info[t]["res"] for t in cyc})
            host = self.host_of(info[victim]["res"])
            self.det_msgs += 2
            r = await n.send(host, n.msg(host, "TASK_ABORT", {"task": victim, "res": info[victim]["res"]}),
                             wait_reply=True, emulate=False, timeout=1.0)
            if r is not None and r.payload.get("aborted"):
                self.victims += 1
            else:
                self.false_aborts += 1            # cycle had already dissolved before the abort landed

    def _victim(self, cycle, info):
        if self.strategy == "detect_random":
            return self.rng.choice(cycle)
        return min(cycle, key=lambda t: (info[t]["prio"] + 0.5 * info[t]["aborts"], -info[t]["since"]))

    async def global_rag(self):
        """Snapshot of the Resource-Allocation Graph from every lock host (for the report/demo)."""
        n, edges = self.node, []
        for h in self.lock_hosts():
            r = await n.send(h, n.msg(h, "WFG_QUERY"), wait_reply=True, emulate=False, timeout=1.0)
            if r:
                edges += [(res, t, "assigned") for res, t in r.payload["holders"].items()]
                edges += [(e["waiter"], e["res"], "request") for e in r.payload["edges"]]
        return edges

    # -------------------------------------------------------- task runner
    async def _run_task(self, spec, client):
        n, tid, prio, gen = self.node, spec["id"], spec["prio"], self.gen
        plan = [tuple(x) for x in spec["plan"]]
        if self.strategy == "prevent":                             # ordered acquisition
            plan.sort(key=lambda s: RANK.index(s[0]) if s[0] in RANK else len(RANK))
        await asyncio.sleep(spec.get("delay", 0))
        t0, aborts, wasted, waited = time.time(), 0, 0.0, 0.0
        while gen == self.gen:
            hosts_used, worked, ok = set(), 0.0, True
            for res, hold in plan:
                host = self.host_of(res)
                tw = time.time()
                hosts_used.add(host)
                r = await n.send(host, n.msg(host, "LOCK_ACQUIRE", {"res": res, "task": tid, "prio": prio,
                                                                    "aborts": aborts, "gen": gen}),
                                 wait_reply=True, timeout=self.lock_timeout)
                waited += time.time() - tw
                if gen != self.gen:
                    return
                if r is None or r.type != "LOCK_GRANTED":
                    ok = False
                    break
                await asyncio.sleep(hold)                           # the work done while holding it
                worked += hold
            for h in hosts_used:                                    # release everything (also on abort)
                await n.send(h, n.msg(h, "LOCK_RELEASE", {"task": tid}), wait_reply=True,
                             emulate=False, timeout=2.0)
            if ok:
                break
            aborts += 1
            wasted += worked
            self.task_aborts += 1
            self.wasted += worked
            n.log.event("task_aborted", task=tid, aborts=aborts, wasted=round(wasted, 3))
            await asyncio.sleep(self.rng.uniform(0.05, 0.2))        # back-off before retry
        if gen == self.gen:
            await n.send(client, n.msg(client, "TASK_DONE", {
                "id": tid, "prio": prio, "node": n.name, "gen": gen, "aborts": aborts,
                "latency": round(time.time() - t0, 3), "wasted": round(wasted, 3),
                "waited": round(waited, 3)}), emulate=False)

    # ---------------------------------------------- race condition experiment
    async def _race(self, p, client):
        """`count` increments of a shared counter that lives on the core.
        unsafe: READ, then WRITE value+1 (another site may write in between -> lost update)
        locked: the same, but inside the distributed lock on resource 'counter'."""
        n, core, gen = self.node, self._core, self.gen
        for i in range(p["count"]):
            tid = f"t{gen}-{n.name}-r{i}"
            if p["mode"] == "locked":
                await n.send(core, n.msg(core, "LOCK_ACQUIRE", {"res": "counter", "task": tid, "gen": gen}),
                             wait_reply=True, timeout=self.lock_timeout)
            r = await n.send(core, n.msg(core, "SHARED_READ", {"key": "dispatches"}), wait_reply=True)
            await asyncio.sleep(0)
            await n.send(core, n.msg(core, "SHARED_WRITE", {"key": "dispatches",
                                                           "value": r.payload["value"] + 1}), wait_reply=True)
            if p["mode"] == "locked":
                await n.send(core, n.msg(core, "LOCK_RELEASE", {"task": tid}), wait_reply=True, emulate=False)
        await n.send(client, n.msg(client, "RACE_DONE", {"mode": p["mode"], "count": p["count"]}), emulate=False)

    def stats(self):
        return {"cc_strategy": self.strategy, "cc_grants": self.grants, "cc_waits": self.waits,
                "cc_aborted_waits": self.aborted_waits, "cc_det_rounds": self.det_rounds,
                "cc_det_msgs": self.det_msgs, "cc_deadlocks": self.deadlocks,
                "cc_victims": self.victims, "cc_false_aborts": self.false_aborts,
                "cc_task_aborts": self.task_aborts, "cc_wasted": round(self.wasted, 3),
                "cc_counter": self.values.get("dispatches", 0)}
