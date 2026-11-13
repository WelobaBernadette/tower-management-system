"""Milestone 4, part 1: Lamport logical clocks vs. skewed wall clocks.

Question: when nodes' physical clocks disagree, can we still tell that a cause
(manager opens an incident) happened before its effect (core receives the
escalation)?  We give mgr1 a clock 0.8 s fast and mgr2 a clock 0.8 s slow, run the
cluster, then check every manager->core incident pair two ways:
  * wall clock : is the core's timestamp later than the manager's?
  * Lamport    : is the core's logical clock larger than the manager's?

    python3 -m experiments.week4_clocks
Writes results/week4_clocks.csv
"""
import asyncio
import copy
import csv
import json
import os
from dataclasses import asdict

from cluster import launch, shutdown, wait_registered
from comms.message import Message
from nodes.node import Node, load_config

BASE = "config/cluster.json"
TMP = "config/_clock_tmp.json"
LOGDIR = "logs/clocks"
RUN_SECONDS = 20
SKEW = {"mgr1": 0.8, "mgr2": -0.8}      # seconds of simulated clock error


def read(path):
    rows = []
    with open(path) as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
    return rows


async def run_cluster():
    cfg = copy.deepcopy(load_config(BASE))
    cfg["log_dir"] = LOGDIR
    cfg["sensor_interval"] = 0.25            # sample often so faults are frequent
    for n in cfg["nodes"]:
        if n["name"] in SKEW:
            n["clock_skew_s"] = SKEW[n["name"]]
    with open(TMP, "w") as f:
        json.dump(cfg, f)
    names = [n["name"] for n in cfg["nodes"] if n["role"] != "client"]
    procs = launch(TMP, names)
    client = Node(cfg, "client")
    await client.start()
    try:
        await wait_registered(client, [n for n in names if n != "core"], timeout=25)
        print(f"Cluster up. Running {RUN_SECONDS}s with mgr1 clock {SKEW['mgr1']:+}s and mgr2 clock {SKEW['mgr2']:+}s ...")
        await asyncio.sleep(RUN_SECONDS)
    finally:
        await shutdown(client, names)
        client.tp.server.close()
        await client.tp.server.wait_closed()
        for p in procs:
            p.wait(timeout=5)


def analyse():
    opened, escalated = {}, {}
    for mgr in ("mgr1", "mgr2"):
        for e in read(f"{LOGDIR}/{mgr}.jsonl"):
            if e["event"] == "correlated_incident":
                opened.setdefault((mgr, e["site"]), []).append(e)
    for e in read(f"{LOGDIR}/core.jsonl"):
        if e["event"] == "incident_escalated":
            escalated.setdefault((e["manager"], e["site"]), []).append(e)
    pairs = []
    for key, lst in opened.items():
        for a, b in zip(lst, escalated.get(key, [])):
            pairs.append((key, a, b))
    pairs.sort(key=lambda p: p[1]["ts"])
    if not pairs:
        print("No incidents occurred; rerun (faults are random).")
        return
    t0 = min(p[1]["ts"] for p in pairs)
    rows = []
    for (mgr, site), a, b in pairs:
        rows.append({"manager": mgr, "site": site,
                     "mgr_wall_s": round(a["ts"] - t0, 3), "core_wall_s": round(b["ts"] - t0, 3),
                     "mgr_lc": a["lc"], "core_lc": b["lc"],
                     "wall_order_ok": b["ts"] > a["ts"], "lamport_order_ok": b["lc"] > a["lc"]})
    n = len(rows)
    wall_ok = sum(r["wall_order_ok"] for r in rows)
    lam_ok = sum(r["lamport_order_ok"] for r in rows)
    os.makedirs("results", exist_ok=True)
    with open("results/week4_clocks.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"\nIncident pairs checked (manager opens -> core receives): {n}")
    print(f"  wall-clock order correct : {wall_ok}/{n}  ({100 * wall_ok / n:.0f}%)")
    print(f"  Lamport order correct    : {lam_ok}/{n}  ({100 * lam_ok / n:.0f}%)")
    print("\nFirst pairs (times in seconds from the first event):")
    print(f"  {'mgr':5} {'site':6} {'mgr wall':>9} {'core wall':>10} {'mgr lc':>7} {'core lc':>8}  wall ok?  lamport ok?")
    for r in rows[:8]:
        print(f"  {r['manager']:5} {r['site']:6} {r['mgr_wall_s']:>9} {r['core_wall_s']:>10} "
              f"{r['mgr_lc']:>7} {r['core_lc']:>8}  {str(r['wall_order_ok']):8}  {r['lamport_order_ok']}")
    m = Message("site1", "mgr1", "SENSOR", {"grid": True, "battery": 55.5, "fuel": 40.1, "link": True}, lc=1234)
    with_lc = len(m.encode())
    d = asdict(m); d.pop("lc")
    without_lc = len((json.dumps(d) + "\n").encode())
    print(f"\nOverhead of the Lamport field: 0 extra messages, {with_lc - without_lc} extra bytes per message "
          f"({with_lc} vs {without_lc}).")
    print("saved results/week4_clocks.csv")


if __name__ == "__main__":
    asyncio.run(run_cluster())
    analyse()
