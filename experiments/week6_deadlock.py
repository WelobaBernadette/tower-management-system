"""Milestone 6: concurrency, mutual exclusion and deadlock management on the real cluster.

All numbers come from real node processes talking over TCP (the seeded link emulator adds delay and
jitter); nothing here is simulated time.

  Part A  Race condition        3 sites each add 40 dispatches to ONE shared counter on the core.
                                unsafe = read then write;  locked = inside the distributed lock.
                                H: unsafe loses updates, locked never does.
  Part B  Deadlock strategies   N dispatch tasks, each holding 2-3 of 4 shared resources spread over
                                core / cloud / mgr1 / mgr2 (hold-and-wait).  Strategies:
                                none | prevent | detect_random (baseline) | detect_priority (proposed)
                                H1: without handling, deadlocks leave tasks stuck forever and the
                                    problem grows with N.
                                H2: detection completes every task but wastes work (aborts).
                                H3: prevention has zero deadlocks and zero aborts, but tasks queue
                                    longer because acquisition order is forced.
                                H4: power-aware victim selection (+aging) finishes critical (power
                                    alarm) tasks sooner than random victims, without hurting
                                    throughput or starving anyone.
  Part C  Detection interval    detect_priority, N=12, detect_interval 0.1 .. 2.0 s.
                                H5: shorter interval = shorter deadlock lifetime, more detector messages.

    python3 -m experiments.week6_deadlock --demo      narrated: race + classic deadlock under each strategy
    python3 -m experiments.week6_deadlock             full run  (add --quick for 1 trial)
Writes results/week6_race.csv, week6_deadlock.csv, week6_detect_interval.csv, week6_rag_deadlock.dot
Controlled variables: 3 sites, 2 managers, link 5 ms +-2 ms, seed from config, task hold 50-150 ms,
25% of tasks critical (priority 3), task start delays 0-0.5 s, same workload seed for every strategy.
"""
import asyncio
import csv
import json
import os
import random
import statistics
import sys
import time

from cluster import launch, shutdown, wait_registered
from kernel.topology import build
from nodes.node import Node, load_config
from services.concurrency import RANK, find_cycles, rag_to_dot

BASE, TMP = "config/cluster.json", "config/_week6_tmp.json"
STRATEGIES = ("none", "prevent", "detect_random", "detect_priority")
SIZES = (4, 8, 16)


async def ask(client, name, typ, payload=None, timeout=3.0):
    return await client.send(name, client.msg(name, typ, payload or {}), wait_reply=True,
                             emulate=False, timeout=timeout)


class Cluster:
    """Start 3 sites + 2 managers + core + cloud, wait for registration and for mgr2 to lead."""

    def __init__(self, base_port=26000):
        self.cfg = build(load_config(BASE), 3, 2, base_port=base_port)
        self.cfg["log_dir"] = "logs/week6"
        self.names = [n["name"] for n in self.cfg["nodes"] if n["role"] != "client"]
        self.sites = [n["name"] for n in self.cfg["nodes"] if n["role"] == "site"]
        self.gen = 0

    async def __aenter__(self):
        with open(TMP, "w") as f:
            json.dump(self.cfg, f)
        self.procs = launch(TMP, self.names)
        self.client = Node(self.cfg, "client")
        await self.client.start()
        await wait_registered(self.client, [n for n in self.names if n != "core"], timeout=40)
        t0 = time.time()
        while time.time() - t0 < 30:                       # wait for the Bully election to settle
            sts = [await ask(self.client, m, "STATS") for m in ("mgr1", "mgr2")]
            if all(sts) and all(s.payload["leader"] == "mgr2" for s in sts):
                return self
            await asyncio.sleep(0.3)
        raise RuntimeError("no leader elected")

    async def __aexit__(self, *exc):
        await shutdown(self.client, self.names)
        self.client.tp.server.close()
        await self.client.tp.server.wait_closed()
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    async def configure(self, strategy, interval=0.5):
        self.gen += 1
        await asyncio.gather(*[ask(self.client, n, "CC_CONFIG", {"strategy": strategy, "interval": interval,
                                                                 "gen": self.gen}) for n in self.names])
        self.client.concurrency.done.clear()
        self.client.concurrency.race_done.clear()
        return self.gen

    async def all_stats(self):
        rs = await asyncio.gather(*[ask(self.client, n, "CC_STATS") for n in self.names])
        return [r.payload for r in rs if r]


# ------------------------------------------------------------------ workloads
def workload(n, seed, gen):
    rng = random.Random(seed)
    out = []
    for i in range(n):
        res = rng.sample(RANK, rng.randint(2, 3))
        out.append({"id": f"t{gen}-{i}", "prio": 3 if rng.random() < 0.25 else 1,
                    "plan": [(r, round(rng.uniform(0.05, 0.15), 3)) for r in res],
                    "delay": round(rng.uniform(0, 0.5), 3)})
    return out


async def run_tasks(cl, specs, strategy, interval=0.25, idle=None, max_s=90.0):
    # 'none' and 'prevent' are judged after 3 s without progress; detection gets longer because it
    # resolves one deadlock per detection round.
    idle = idle or (3.0 if strategy in ("none", "prevent") else 8.0)
    gen = await cl.configure(strategy, interval)
    for s in specs:
        s["id"] = f"t{gen}-{s['id'].split('-')[1]}"
    done = cl.client.concurrency.done
    t0 = time.time()
    for i, s in enumerate(specs):
        await ask(cl.client, cl.sites[i % len(cl.sites)], "TASK_START", s)
    n, last, seen = len(specs), time.time(), 0
    while time.time() - t0 < max_s:
        await asyncio.sleep(0.1)
        if len(done) > seen:
            seen, last = len(done), time.time()
        if seen == n or (time.time() - last > idle and time.time() - t0 > 1.5):
            break
    done_rows = list(done.values())
    makespan = (max(0.0, last - t0) if seen == n else time.time() - t0)
    st = await cl.all_stats()
    stuck_cycles = 0
    if strategy in ("none", "prevent"):             # nobody is looking: count cycles still standing
        edges = await cl.client.concurrency.global_rag()
        holder = {a: b for a, b, k in edges if k == "assigned"}
        stuck_cycles = len(find_cycles({a: holder[b] for a, b, k in edges if k == "request" and b in holder}))
    mean = lambda xs: round(statistics.mean(xs), 3) if xs else ""
    crit = [r["latency"] for r in done_rows if r["prio"] == 3]
    rout = [r["latency"] for r in done_rows if r["prio"] == 1]
    return {"n_tasks": n, "strategy": strategy, "completed": seen, "stuck": n - seen,
            "deadlocks": sum(s["cc_deadlocks"] for s in st) + stuck_cycles,
            "victims": sum(s["cc_victims"] for s in st),
            "false_aborts": sum(s["cc_false_aborts"] for s in st),
            "task_aborts": sum(s["cc_task_aborts"] for s in st),
            "wasted_s": round(sum(s["cc_wasted"] for s in st), 3),
            "makespan_s": round(makespan, 3),
            "throughput_tasks_s": round(seen / makespan, 2) if makespan else 0,
            "critical_latency_s": mean(crit), "routine_latency_s": mean(rout),
            "max_waited_s": max((r["waited"] for r in done_rows), default=""),
            "max_aborts_one_task": max((r["aborts"] for r in done_rows), default=0),
            "det_rounds": sum(s["cc_det_rounds"] for s in st), "det_msgs": sum(s["cc_det_msgs"] for s in st)}


async def run_race(cl, mode, count=40):
    await cl.configure("none")
    for s in cl.sites:
        await ask(cl.client, s, "RACE_RUN", {"mode": mode, "count": count})
    t0 = time.time()
    while len(cl.client.concurrency.race_done) < len(cl.sites) and time.time() - t0 < 60:
        await asyncio.sleep(0.1)
    r = await ask(cl.client, "core", "SHARED_READ", {"key": "dispatches"})
    expected, actual = count * len(cl.sites), r.payload["value"]
    return {"mode": mode, "expected": expected, "actual": actual, "lost_updates": expected - actual,
            "seconds": round(time.time() - t0, 2)}


# ----------------------------------------------------------------------- CSV
def write_csv(path, rows):
    os.makedirs("results", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print("saved", path)


def mean_by(rows, keys, fields):
    out = {}
    for r in rows:
        out.setdefault(tuple(r[k] for k in keys), []).append(r)
    for k, g in out.items():
        yield k, {f: (round(statistics.mean(x[f] for x in g if x[f] != ""), 2)
                      if any(x[f] != "" for x in g) else "-") for f in fields}


# ---------------------------------------------------------------------- demo
CLASSIC = [{"id": "t0-0", "prio": 3, "delay": 0, "plan": [("crew", 0.4), ("fuel_truck", 0.4)]},
           {"id": "t0-1", "prio": 1, "delay": 0, "plan": [("fuel_truck", 0.4), ("crew", 0.4)]}]


async def demo(cl):
    print("1) RACE CONDITION: 3 sites x 40 increments of one shared counter on the core")
    for mode in ("unsafe", "locked"):
        r = await run_race(cl, mode)
        print(f"   {mode:7} expected {r['expected']}  got {r['actual']}  lost updates {r['lost_updates']}")
    print("\n2) CLASSIC DEADLOCK: critical task A wants crew then fuel_truck; routine task B wants fuel_truck then crew")
    for strat in ("none", "detect_priority", "prevent"):
        specs = [dict(s) for s in CLASSIC]
        gen_hint = cl.gen + 1
        if strat == "none":
            await cl.configure("none")
            for i, s in enumerate(specs):
                s["id"] = f"t{cl.gen}-{i}"
                await ask(cl.client, cl.sites[i], "TASK_START", s)
            await asyncio.sleep(1.5)
            edges = await cl.client.concurrency.global_rag()
            holder = {a: b for a, b, k in edges if k == "assigned"}
            graph = {a: holder[b] for a, b, k in edges if k == "request" and b in holder}
            cyc = (find_cycles(graph) or [[]])[0]
            print(f"   [none] after 1.5 s both tasks are stuck. Resource-Allocation Graph edges:")
            for a, b, k in sorted(edges):
                print(f"      {a} -> {b}   ({k})")
            print(f"      wait-for cycle: {' -> '.join(cyc + cyc[:1])}  => DEADLOCK")
            os.makedirs("results", exist_ok=True)
            with open("results/week6_rag_deadlock.dot", "w") as f:
                f.write(rag_to_dot(edges, cyc, "deadlock"))
            print("      wrote results/week6_rag_deadlock.dot  (render: dot -Tpng results/week6_rag_deadlock.dot -o rag.png)")
        else:
            r = await run_tasks(cl, specs, strat)
            who = {k: v for k, v in cl.client.concurrency.done.items()}
            ab = {k.split("-")[1]: v["aborts"] for k, v in who.items()}
            print(f"   [{strat}] completed {r['completed']}/2, deadlocks detected {r['deadlocks']}, "
                  f"aborts (task0=critical, task1=routine): {ab}, makespan {r['makespan_s']} s")


async def main():
    quick = "--quick" in sys.argv
    trials = 1 if quick else 3
    async with Cluster() as cl:
        if "--demo" in sys.argv:
            await demo(cl)
            return
        rows = [dict(trial=t, **await run_race(cl, m)) for m in ("unsafe", "locked") for t in range(trials)]
        write_csv("results/week6_race.csv", rows)
        for (m,), v in mean_by(rows, ["mode"], ["lost_updates", "actual", "seconds"]):
            print(f"  race {m:7} mean lost updates {v['lost_updates']} of {rows[0]['expected']}")

        rows = []
        for n in SIZES:
            for t in range(trials):
                specs = workload(n, 1000 + n * 10 + t, 0)
                for strat in STRATEGIES:
                    r = await run_tasks(cl, [dict(s) for s in specs], strat)
                    rows.append({"trial": t, **r})
                    print(f"  N={n:2} t{t} {strat:15} done {r['completed']:2}/{n} dl {r['deadlocks']:2} "
                          f"aborts {r['task_aborts']:2} span {r['makespan_s']:6.2f}s crit {r['critical_latency_s']}")
        write_csv("results/week6_deadlock.csv", rows)
        fields = ["completed", "stuck", "deadlocks", "task_aborts", "wasted_s", "makespan_s",
                  "throughput_tasks_s", "critical_latency_s", "routine_latency_s", "max_aborts_one_task", "det_msgs"]
        print(f"\n{'N':>3} {'strategy':15}" + "".join(f"{f[:12]:>13}" for f in fields))
        for (n, s), v in mean_by(rows, ["n_tasks", "strategy"], fields):
            print(f"{n:>3} {s:15}" + "".join(f"{v[f]:>13}" for f in fields))

        rows = []
        for interval in (0.1, 0.25, 0.5, 1.0, 2.0):
            for t in range(trials):
                r = await run_tasks(cl, workload(12, 5000 + t, 0), "detect_priority", interval=interval)
                rows.append({"detect_interval_s": interval, "trial": t, **r})
        write_csv("results/week6_detect_interval.csv", rows)
        for (i,), v in mean_by(rows, ["detect_interval_s"], ["makespan_s", "task_aborts", "wasted_s", "det_msgs"]):
            print(f"  interval {i:>4}s  makespan {v['makespan_s']}  aborts {v['task_aborts']}  wasted {v['wasted_s']}  det_msgs {v['det_msgs']}")


if __name__ == "__main__":
    asyncio.run(main())
