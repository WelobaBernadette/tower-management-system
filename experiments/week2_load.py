"""Week 2: distributed processing + performance measurement.

Hypothesis: spreading jobs across nodes (round_robin / least_loaded) gives higher
throughput and lower latency than placing everything on one node ('first'), and the
gap grows with request rate.

    python -m experiments.week2_load
Writes results/week2_results.csv
"""
import asyncio
import copy
import csv
import json
import os
import statistics
import subprocess
import sys
import time

from cluster import launch, shutdown, wait_registered
from nodes.node import Node, load_config

BASE = "config/cluster.json"
TMP = "config/_tmp.json"


async def run_once(cfg, placement, rate, n_jobs=200, burst=0.05, loss=0.0):
    cfg = copy.deepcopy(cfg)
    cfg["placement"] = placement
    cfg["link"]["loss"] = loss
    cfg["log_dir"] = f"logs/{placement}_{rate}"
    with open(TMP, "w") as f:
        json.dump(cfg, f)
    names = [n["name"] for n in cfg["nodes"] if n["role"] != "client"]
    procs = launch(TMP, names)
    client = Node(cfg, "client")
    await client.start()
    try:
        await wait_registered(client, [n for n in names if n != "core"])
        await asyncio.sleep(1.0)  # let services and CPU samplers warm up
        submit = {}
        t0 = time.time()
        for i in range(n_jobs):
            jid = f"j{i}"
            submit[jid] = time.time()
            # fire-and-forget so the offered rate is not limited by link delay
            asyncio.create_task(client.send("core", client.msg("core", "SPAWN_REQUEST", {
                "job_id": jid, "cpu": 10, "mem": 32, "burst": burst, "prio": 5,
                "client": "client", "submit_ts": submit[jid]})))  # emulated link
            await asyncio.sleep(1.0 / rate)
        deadline = time.time() + 10
        while time.time() < deadline and len(client.done) < n_jobs:
            await asyncio.sleep(0.1)

        ok = [(v[0], v[0] - v[1]["submit_ts"]) for v in client.done.values() if v[1]["status"] == "ok"]
        refused = sum(1 for v in client.done.values() if v[1]["status"] == "refused")
        lat = [x[1] for x in sorted(ok)]  # latency in completion order
        t_end = max((x[0] for x in ok), default=t0 + 1)
        stats = []
        for n in names:
            r = await client.send(n, client.msg(n, "STATS"), wait_reply=True, emulate=False)
            stats.append(r.payload)
        workers = [s for s in stats if s["role"] in ("site", "manager")]
        sent = sum(s["link_sent"] for s in stats) + client.link.sent
        dropped = sum(s["link_dropped"] for s in stats) + client.link.dropped
        lat_sorted = sorted(lat)
        return {
            "placement": placement, "rate": rate, "jobs": n_jobs, "completed": len(ok),
            "refused": refused, "lost": n_jobs - len(ok) - refused,
            "throughput_jobs_s": round(len(ok) / (t_end - t0), 2),
            "latency_mean_ms": round(1000 * statistics.mean(lat), 1) if lat else None,
            "latency_p95_ms": round(1000 * lat_sorted[int(0.95 * (len(lat_sorted) - 1))], 1) if lat else None,
            "jitter_ms": round(1000 * statistics.pstdev(lat), 1) if len(lat) > 1 else None,
            "packet_loss_pct": round(100 * dropped / max(sent, 1), 2),
            "cpu_mean_pct": round(statistics.mean(s["cpu_mean"] for s in workers), 1),
            "mem_max_mb": round(max(s["rss_max_mb"] for s in workers), 1),
            "jobs_per_node": {s["node"]: s["completed"] for s in workers},
        }
    finally:
        await shutdown(client, names)
        client.tp.server.close()  # free the client port for the next run
        await client.tp.server.wait_closed()
        for p in procs:
            p.wait(timeout=5)


async def main():
    cfg = load_config(BASE)
    os.makedirs("results", exist_ok=True)
    rows = []
    for placement in ("first", "round_robin", "least_loaded"):
        for rate in (20, 60, 100):
            r = await run_once(cfg, placement, rate)
            rows.append(r)
            print(placement, rate, {k: r[k] for k in ("throughput_jobs_s", "latency_mean_ms", "latency_p95_ms",
                                                        "jitter_ms", "refused", "lost", "cpu_mean_pct")})
    with open("results/week2_results.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print("saved results/week2_results.csv")


if __name__ == "__main__":
    asyncio.run(main())
