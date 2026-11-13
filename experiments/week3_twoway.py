"""Milestone 3, part 2: two-way flow between the core and the cloud.

Hypothesis: letting the cloud push analytics back to the core (watch-list of repeat-offender
sites) lets the core change how it handles new incidents, at a small extra message cost and
with delays of a few tens of milliseconds in each direction.

Independent variable : cloud_feedback on
Dependent variables  : up-path latency (core -> cloud), down-path latency (cloud -> core),
                       incidents the core flagged high priority, extra messages versus the
                       normal coordination traffic
Controlled variables : 3 sites, 2 managers, seed 42, sensor readings every 0.2 s, 20 s run

    python3 -m experiments.week3_twoway
Writes results/week3_twoway.csv (one row per policy update)
"""
import asyncio
import csv
import json
import os
import statistics
import time

from cluster import launch, shutdown, wait_registered
from kernel.topology import build
from nodes.node import Node, load_config

BASE = "config/cluster.json"
TMP = "config/_twoway_tmp.json"
DURATION = 20
SITES, MANAGERS = 3, 2


def read(path):
    rows = []
    try:
        with open(path) as f:
            for line in f:
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    pass
    except OSError:
        pass
    return rows


async def ask(client, name, typ, payload=None):
    r = await client.send(name, client.msg(name, typ, payload or {}), wait_reply=True, emulate=False, timeout=2.0)
    return r.payload if r else None


async def main():
    base_cfg = load_config(BASE)
    cfg = build(base_cfg, SITES, MANAGERS, base_port=25000)
    cfg.update({"cloud_feedback": True, "watch_threshold": 2, "policy_interval": 0.5,
                "sensor_interval": 0.2, "log_dir": "logs/twoway"})
    with open(TMP, "w") as f:
        json.dump(cfg, f)
    names = [n["name"] for n in cfg["nodes"] if n["role"] != "client"]
    procs = launch(TMP, names)
    client = Node(cfg, "client")
    await client.start()
    try:
        await wait_registered(client, [n for n in names if n != "core"], timeout=40)
        print(f"Cluster up ({SITES} sites, {MANAGERS} managers). Running {DURATION} s with cloud feedback on ...")
        await asyncio.sleep(DURATION)

        core = await ask(client, "core", "STATS")
        cloud = await ask(client, "cloud", "STATS")
        pulls = {f"site{i + 1}": await ask(client, "cloud", "HISTORY_QUERY", {"site": f"site{i + 1}"})
                 for i in range(SITES)}
        all_stats = [await ask(client, n, "STATS") for n in names]
        coord_total = sum(s.get("coord_msgs", 0) for s in all_stats if s)
    finally:
        await shutdown(client, names)
        client.tp.server.close()
        await client.tp.server.wait_closed()
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    d = cfg["log_dir"]
    cloud_log, core_log = read(f"{d}/cloud.jsonl"), read(f"{d}/core.jsonl")
    up = [e["up_ms"] for e in cloud_log if e.get("event") == "history_recorded"]
    pushed = {e["seq"]: e for e in cloud_log if e.get("event") == "policy_pushed"}
    recvd = {e["seq"]: e for e in core_log if e.get("event") == "policy_received"}
    t0 = min([e["ts"] for e in pushed.values()] or [0])
    rows, down = [], []
    for seq in sorted(pushed):
        if seq in recvd:
            ms = round(1000 * (recvd[seq]["ts"] - pushed[seq]["ts"]), 1)
            down.append(ms)
            rows.append({"policy_seq": seq, "pushed_at_s": round(pushed[seq]["ts"] - t0, 2),
                         "latency_ms": ms, "watchlist": " ".join(recvd[seq].get("watchlist", []))})
    os.makedirs("results", exist_ok=True)
    with open("results/week3_twoway.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["policy_seq", "pushed_at_s", "latency_ms", "watchlist"])
        w.writeheader()
        w.writerows(rows)

    fb_msgs = core["fb_records_sent"] + cloud["fb_policies_sent"]
    print("\nUP    core -> cloud   incident records:", core["fb_records_sent"],
          f"| mean latency {statistics.mean(up):.1f} ms" if up else "| none recorded")
    print("DOWN  cloud -> core   policy updates:  ", cloud["fb_policies_sent"], "sent,",
          core["fb_policies_received"], "received",
          f"| mean latency {statistics.mean(down):.1f} ms" if down else "| none")
    print("      final watch-list at core:", core["fb_watchlist"] or "(empty: no site reached 2 incidents)")
    print("LOOP  incidents the core flagged HIGH priority because of the cloud:", core["fb_boosted"])
    print("PULL  HISTORY_QUERY answers from the cloud:")
    for s, r in pulls.items():
        if r:
            print(f"      {s}: {r['incidents']} incidents, last cause {r['last_cause']}, watch-listed {r['watch_listed']}")
    print(f"COST  feedback messages {fb_msgs} vs coordination messages {coord_total} "
          f"({100 * fb_msgs / max(coord_total, 1):.0f}% extra)")
    if not rows:
        print("NOTE: no policy was pushed. Run longer (raise DURATION) or lower watch_threshold.")
    print("saved results/week3_twoway.csv")


if __name__ == "__main__":
    asyncio.run(main())
