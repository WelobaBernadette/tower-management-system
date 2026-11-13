"""Distributed transactions (Milestone 5): two-phase commit and three-phase commit.

One transaction = "dispatch field team T to incident I at site S". It must change state in
THREE places, and either all three change or none does:

    owner manager  records  incident -> team   (resource lock: incident:<id>)   [the manager of site S]
    core           marks    team busy          (resource lock: team:<id>)       [free again after dispatch_hold_s]
    cloud          appends  an audit entry     (no lock needed)

The coordinator is the manager the client sends DISPATCH to (the Bully leader; any other manager
answers NOT_LEADER).

2PC   PREPARE -> votes -> COMMIT/ABORT -> ACK            (4 messages per participant)
3PC   PREPARE -> votes -> PRECOMMIT -> ACK -> COMMIT     (6 messages per participant)

Participant states: PREPARED -> (PRECOMMITTED in 3PC) -> COMMITTED | ABORTED.
Every state change is written to a write-ahead log (<log_dir>/<node>.wal) BEFORE it is acted on, so a
restarted node rebuilds its state (and re-takes its locks) and finishes or aborts what was in flight.

Recovery and termination
  * Isolation: a participant votes NO if the resource is already locked or already taken.
  * A participant stuck in PREPARED/PRECOMMITTED runs the cooperative termination protocol:
      - coordinator reachable   -> ask it for the decision, otherwise keep waiting
      - coordinator unreachable -> ask the other participants for their states:
            2PC: follow any COMMITTED/ABORTED; someone never voted -> abort;
                 everyone merely PREPARED -> BLOCKED (logged, keeps polling until the coordinator is back)
            3PC: any COMMITTED -> commit; any ABORTED -> abort;
                 any PRECOMMITTED -> commit; all merely PREPARED -> abort       (never blocks)
  * Coordinator: keeps re-sending its decision until every participant has acknowledged (then COMPLETE).
    A restarted coordinator reads its WAL: a logged decision is re-sent; with no decision, 2PC presumes
    abort and 3PC asks the participants for their states and applies the 3PC rule.

Fault injection (all optional)
  per request, in DISPATCH payload faults={...}:
      crash_at: after_votes | partial_precommit (3PC) | after_precommit (3PC) | after_decision   coordinator dies
      crash_before_vote: <node>   that participant dies before voting
      crash_after_vote:  <node>   that participant dies shortly (txn_crash_delay s) after voting YES
      vote_no: <node>             that participant votes NO
  per cluster, in config: cfg["txn_fault"] = {"node","point","txid"}  hard-kills that process at
      "before_vote" (participant) or "after_votes" / "after_precommit" / "after_decision" (coordinator).
"""
import asyncio
import json
import os
import time

FINAL = ("COMMITTED", "ABORTED")
OPEN = ("PREPARED", "PRECOMMITTED")


class TransactionService:
    def __init__(self, node):
        self.node = node
        cfg = node.cfg
        self.timeout = cfg.get("txn_timeout", 1.0)          # wait this long for a decision, then terminate
        self.vote_timeout = cfg.get("txn_vote_timeout", 1.0)
        self.poll = cfg.get("txn_poll", 0.5)
        self.hold_s = cfg.get("dispatch_hold_s", 0.3)        # how long a dispatched team stays busy
        self.crash_delay = cfg.get("txn_crash_delay", 0.05)  # crash_after_vote: delay after the vote reply
        self.fault = cfg.get("txn_fault") or {}
        self.cloud = next((n["name"] for n in cfg["nodes"] if n["role"] == "cloud"), None)
        self.state, self.info, self.locks = {}, {}, {}       # participant side
        self.teams, self.assigned, self.audit = {}, {}, []   # committed effects (core / manager / cloud)
        self.coord, self.decision, self.running = {}, {}, set()   # coordinator side
        self.blocked = set()                                 # transactions this node is blocked on (2PC)
        self.msgs = self.term_msgs = self.commits = self.aborts = self.violations = 0
        log_dir = cfg.get("log_dir", "logs")
        os.makedirs(log_dir, exist_ok=True)
        self.wal_path = os.path.join(log_dir, f"{node.name}.wal")
        self._replay()
        self.wal = open(self.wal_path, "a", buffering=1)

    # ---------- what each node role owns ----------
    def _resources(self, op):
        role = self.node.role
        if role == "core":
            return [f"team:{op['team']}"]
        if role == "manager":
            return [f"incident:{op['incident']}"]
        return []

    def _conflict(self, op):
        role = self.node.role
        if role == "core":
            return self.teams.get(op["team"], {}).get("state") == "busy"
        if role == "manager":
            return op["incident"] in self.assigned
        return False

    def _apply(self, txid, op, schedule=True):
        role = self.node.role
        if role == "core":
            self.teams[op["team"]] = {"state": "busy", "tx": txid, "incident": op["incident"]}
            if schedule:
                asyncio.create_task(self._free_later(op["team"], txid))
        elif role == "manager":
            self.assigned[op["incident"]] = op["team"]
        elif role == "cloud" and txid not in self.audit:
            self.audit.append(txid)

    async def _free_later(self, team, txid):
        await asyncio.sleep(self.hold_s)                     # the team is out on the job, then free again
        t = self.teams.get(team)
        if t and t["tx"] == txid and t["state"] == "busy":
            t["state"], t["tx"] = "free", None

    # ---------- write-ahead log ----------
    def _w(self, **rec):
        self.wal.write(json.dumps({"ts": time.time(), **rec}) + "\n")

    def _replay(self):
        if not os.path.exists(self.wal_path):
            return
        with open(self.wal_path) as f:
            for line in f:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                t = r["t"]
                if r["r"] == "p":
                    if "op" in r:
                        self.info[t] = {"op": r["op"], "coord": r["coord"], "parts": r["parts"], "proto": r["proto"]}
                    self.state[t] = r["st"]
                elif r["ev"] == "BEGIN":
                    self.coord[t] = {"proto": r["proto"], "parts": r["parts"], "op": r["op"], "done": False}
                elif r["ev"] in ("COMMIT", "ABORT"):
                    self.decision[t] = r["ev"]
                elif r["ev"] == "COMPLETE":
                    self.coord.setdefault(t, {})["done"] = True
        for t, st in self.state.items():
            info = self.info.get(t)
            if not info:
                continue
            if st in OPEN:
                for res in self._resources(info["op"]):
                    self.locks[res] = t                      # still in doubt: keep the locks
            elif st == "COMMITTED":
                self._apply(t, info["op"], schedule=False)

    def start(self):
        """Called when the node starts. After a restart: resume anything that was in flight."""
        for t, st in list(self.state.items()):
            if st in OPEN:
                asyncio.create_task(self._guard(self._watchdog(t)))
        for team, rec in list(self.teams.items()):
            if rec["state"] == "busy":
                asyncio.create_task(self._free_later(team, rec["tx"]))
        if any(not c.get("done") for c in self.coord.values()):
            asyncio.create_task(self._guard(self._recover()))

    async def _guard(self, coro):
        try:
            await coro
        except Exception as e:                              # keep background failures visible in the log
            self.node.log.event("txn_task_error", error=repr(e))

    # ---------- fault injection ----------
    def _die(self, point, txid):
        self.node.log.event("txn_fault_crash", point=point, txid=txid, lc=self.node.lc)
        self.wal.flush()
        os._exit(137)

    def _maybe_crash(self, point, txid, faults=None):
        f = self.fault
        if f and f.get("node") == self.node.name and f.get("point") == point and f.get("txid") in (None, txid):
            self._die(point, txid)
        if faults and faults.get("crash_at") == point:       # coordinator-side, requested per dispatch
            self._die(point, txid)

    # ---------- message entry point (called from Node.handle) ----------
    async def handle(self, m):
        t, p, n = m.type, m.payload, self.node
        if t == "DISPATCH":
            return await self.on_dispatch(m)
        if t == "TXN_PREPARE":
            return self.on_prepare(m)
        if t == "TXN_PRECOMMIT":
            return self.on_precommit(m)
        if t in ("TXN_COMMIT", "TXN_ABORT"):
            st = self._finish(p["txid"], "COMMIT" if t == "TXN_COMMIT" else "ABORT")
            return n.msg(m.sender, "TXN_ACK", {"txid": p["txid"], "state": st})
        if t == "TXN_STATE_REQ":
            return n.msg(m.sender, "TXN_STATE", {"txid": p["txid"], "state": self.state.get(p["txid"]),
                                                  "locks": len(self.locks)})
        if t == "TXN_DECISION_REQ":
            return n.msg(m.sender, "TXN_DECISION", {"txid": p["txid"], "decision": self._coord_decision(p["txid"])})
        if t == "TXN_DUMP":
            return n.msg(m.sender, "TXN_DUMP", {
                "node": n.name, "role": n.role, "states": dict(self.state),
                "busy": [k for k, v in self.teams.items() if v["state"] == "busy"],
                "reserved": [r for r in self.locks if r.startswith("team:")],
                "assigned": len(self.assigned), "audit": len(self.audit), "locks": len(self.locks),
                "violations": self.violations, "blocked": sorted(self.blocked),
                "in_doubt": [k for k, v in self.state.items() if v in OPEN]})
        return None

    # ---------- participant ----------
    def on_prepare(self, m):
        p = m.payload
        txid, op, faults = p["txid"], p["op"], p.get("faults") or {}
        me = self.node.name
        if faults.get("crash_before_vote") == me:
            self._die("before_vote", txid)
        self._maybe_crash("before_vote", txid)
        st = self.state.get(txid)
        if st in OPEN or st == "COMMITTED":
            vote = "YES"                                     # retry of a PREPARE we already answered
        elif st == "ABORTED":
            vote = "NO"
        else:
            res = self._resources(op)
            if faults.get("vote_no") == me or any(self.locks.get(r) not in (None, txid) for r in res) \
                    or self._conflict(op):
                vote, new = "NO", "ABORTED"                  # forced NO, or isolation: the resource is taken
                self.aborts += 1
            else:
                vote, new = "YES", "PREPARED"
                for r in res:
                    self.locks[r] = txid
            self.state[txid] = new
            self.info[txid] = {"op": op, "coord": p["coord"], "parts": p["parts"], "proto": p["proto"]}
            self._w(r="p", t=txid, st=new, op=op, coord=p["coord"], parts=p["parts"], proto=p["proto"])
            self.node.log.event("txn_state", txid=txid, state=new, vote=vote)
            if new == "PREPARED":
                asyncio.create_task(self._guard(self._watchdog(txid)))
        if vote == "YES" and faults.get("crash_after_vote") == me:
            asyncio.get_running_loop().call_later(self.crash_delay, os._exit, 1)
        return self.node.msg(m.sender, "TXN_VOTE", {"txid": txid, "vote": vote})

    def on_precommit(self, m):
        txid = m.payload["txid"]
        if self.state.get(txid) == "PREPARED":
            self.state[txid] = "PRECOMMITTED"
            self._w(r="p", t=txid, st="PRECOMMITTED")
            self.node.log.event("txn_state", txid=txid, state="PRECOMMITTED")
        return self.node.msg(m.sender, "TXN_ACK", {"txid": txid, "state": self.state.get(txid)})

    def _finish(self, txid, decision):
        st, new = self.state.get(txid), ("COMMITTED" if decision == "COMMIT" else "ABORTED")
        if st in FINAL:
            if st != new:
                self.violations += 1                         # tried to flip a final decision
                self.node.log.event("txn_inconsistent", txid=txid, have=st, got=new)
            return st
        info = self.info.get(txid)
        if new == "COMMITTED" and info:
            self._apply(txid, info["op"])
        for r in [r for r, owner in self.locks.items() if owner == txid]:
            del self.locks[r]
        self.state[txid] = new
        self._w(r="p", t=txid, st=new)
        self.commits += new == "COMMITTED"
        self.aborts += new == "ABORTED"
        self.blocked.discard(txid)
        self.node.log.event("txn_state", txid=txid, state=new)
        return new

    async def _watchdog(self, txid):
        await asyncio.sleep(self.timeout)
        while self.state.get(txid) in OPEN:
            await self._terminate(txid)
            if self.state.get(txid) in OPEN:
                await asyncio.sleep(self.poll)

    async def _terminate(self, txid):
        info, n = self.info.get(txid), self.node
        if not info:
            return
        coord, proto = info["coord"], info["proto"]
        self.term_msgs += 1
        r = await n.send(coord, n.msg(coord, "TXN_DECISION_REQ", {"txid": txid}), wait_reply=True, timeout=0.8)
        if r is not None:                       # coordinator is alive: it will finish the job
            if r.payload.get("decision"):
                self._finish(txid, r.payload["decision"])
            return
        states = {n.name: self.state.get(txid)}
        for p in info["parts"]:
            if p != n.name:
                self.term_msgs += 1
                rr = await n.send(p, n.msg(p, "TXN_STATE_REQ", {"txid": txid}), wait_reply=True, timeout=0.8)
                if rr is not None:
                    states[p] = rr.payload.get("state")
        vals = set(states.values())
        n.log.event("txn_terminate", txid=txid, proto=proto, states=states, lc=n.lc)
        if "COMMITTED" in vals:
            self._finish(txid, "COMMIT")
        elif "ABORTED" in vals:
            self._finish(txid, "ABORT")
        elif proto == "3pc":
            self._finish(txid, "COMMIT" if "PRECOMMITTED" in vals else "ABORT")
        elif None in vals:                      # 2PC: someone never voted, so nobody can have committed
            self._finish(txid, "ABORT")
        elif txid not in self.blocked:          # 2PC with everyone uncertain: BLOCKED until the coordinator is back
            self.blocked.add(txid)
            n.log.event("txn_blocked", txid=txid, states=states, lc=n.lc)

    # ---------- coordinator ----------
    def _coord_decision(self, txid):
        if txid in self.decision:
            return self.decision[txid]
        if txid in self.running:
            return None                          # still deciding
        c = self.coord.get(txid)
        return "ABORT" if (c is None or c.get("proto") == "2pc") else None   # presumed abort (2PC only)

    async def on_dispatch(self, m):
        p, el = m.payload, self.node.election
        if el and el.leader not in (None, self.node.name):
            return self.node.msg(m.sender, "DISPATCH_RESULT", {"txid": p["txid"], "outcome": "NOT_LEADER", "leader": el.leader})
        t0 = time.time()
        outcome, msgs = await self.coordinate(p.get("proto", "2pc"), p["txid"],
                                              {"incident": p["incident"], "site": p["site"], "team": p["team"]},
                                              p.get("faults") or {})
        return self.node.msg(m.sender, "DISPATCH_RESULT", {
            "txid": p["txid"], "outcome": outcome, "proto": p.get("proto", "2pc"), "msgs": msgs,
            "latency_ms": round((time.time() - t0) * 1000, 1)})

    async def coordinate(self, proto, txid, op, faults):
        n = self.node
        owner = next(x["manager"] for x in n.cfg["nodes"] if x["name"] == op["site"])
        parts = [owner, n.core_name, self.cloud]
        self.coord[txid] = {"proto": proto, "parts": parts, "op": op, "done": False}
        self.running.add(txid)
        self._w(r="c", t=txid, ev="BEGIN", proto=proto, parts=parts, op=op)
        count = [0]

        async def ask(p, typ, extra=None):
            count[0] += 1
            r = await n.send(p, n.msg(p, typ, {"txid": txid, **(extra or {})}), wait_reply=True,
                             timeout=self.vote_timeout)
            count[0] += r is not None
            return r

        votes = await asyncio.gather(*[ask(p, "TXN_PREPARE", {"op": op, "parts": parts, "proto": proto,
                                                              "coord": n.name, "faults": faults}) for p in parts])
        yes = all(v is not None and v.payload.get("vote") == "YES" for v in votes)
        self._maybe_crash("after_votes", txid, faults)
        if yes and proto == "3pc":
            self._w(r="c", t=txid, ev="PRECOMMIT")
            if faults.get("crash_at") == "partial_precommit":     # PRECOMMIT reaches only the first participant
                await ask(parts[0], "TXN_PRECOMMIT")
                self._die("partial_precommit", txid)
            await asyncio.gather(*[ask(p, "TXN_PRECOMMIT") for p in parts])
            self._maybe_crash("after_precommit", txid, faults)
        dec = "COMMIT" if yes else "ABORT"
        self.decision[txid] = dec
        self._w(r="c", t=txid, ev=dec)                       # decision is logged BEFORE anyone is told
        self._maybe_crash("after_decision", txid, faults)
        acks = await asyncio.gather(*[ask(p, "TXN_COMMIT" if dec == "COMMIT" else "TXN_ABORT") for p in parts])
        if all(a is not None for a in acks):
            self._w(r="c", t=txid, ev="COMPLETE")
            self.coord[txid]["done"] = True
        else:                                                # keep re-sending until everybody has acknowledged
            asyncio.create_task(self._guard(self._send_decision(txid, dec, parts)))
        self.running.discard(txid)
        self.msgs += count[0]
        return ("COMMITTED" if dec == "COMMIT" else "ABORTED"), count[0]

    async def _send_decision(self, txid, dec, parts, tries=40):
        n, todo = self.node, list(parts)
        for _ in range(tries):
            for p in list(todo):
                self.term_msgs += 1
                r = await n.send(p, n.msg(p, "TXN_COMMIT" if dec == "COMMIT" else "TXN_ABORT", {"txid": txid}),
                                 wait_reply=True, timeout=0.8)
                if r is not None:
                    todo.remove(p)
            if not todo:
                self._w(r="c", t=txid, ev="COMPLETE")
                if txid in self.coord:
                    self.coord[txid]["done"] = True
                return
            await asyncio.sleep(self.poll)

    async def _decide_3pc(self, txid, parts):
        """Restarted 3PC coordinator with no logged decision: apply the 3PC rule to the participants' states."""
        n, states = self.node, []
        for p in parts:
            r = await n.send(p, n.msg(p, "TXN_STATE_REQ", {"txid": txid}), wait_reply=True, timeout=0.8)
            if r is not None:
                states.append(r.payload.get("state"))
        if not states:
            return None
        if "COMMITTED" in states or "PRECOMMITTED" in states:
            return "COMMIT"
        return "ABORT"

    async def _recover(self):
        """Coordinator restarted: finish every transaction that has no COMPLETE record."""
        await asyncio.sleep(0.5)
        for txid, c in list(self.coord.items()):
            if c.get("done"):
                continue
            dec = self.decision.get(txid)
            if dec is None:
                dec = "ABORT" if c.get("proto") == "2pc" else await self._decide_3pc(txid, c["parts"])   # presumed abort
                if dec is None:
                    self.node.log.event("txn_recover_deferred", txid=txid)
                    continue
                self.decision[txid] = dec
                self._w(r="c", t=txid, ev=dec)
            self.node.log.event("txn_recover", txid=txid, decision=dec)
            await self._send_decision(txid, dec, c["parts"])

    def stats(self):
        return {"txn_msgs": self.msgs, "txn_term_msgs": self.term_msgs, "txn_commits": self.commits,
                "txn_aborts": self.aborts, "txn_locks": len(self.locks), "txn_blocked": len(self.blocked),
                "txn_violations": self.violations,
                "txn_in_doubt": sum(1 for v in self.state.values() if v in OPEN)}
