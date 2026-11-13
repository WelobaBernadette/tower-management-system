"""Milestone 3: scalability analysis.

Hypothesis: as the cluster grows (more edge sites / more managers), job
throughput and latency should degrade gracefully rather than collapse, but
coordination overhead (registration + resource + archive + incident messages
relative to cluster size) grows with the number of nodes.

    python -m experiments.week3_scalability
Writes results/week3_scalability.csv
"""
import asyncio
import csv
import json
import os
import statistics
import sys
import time

from cluster import launch, shutdown, wait_registered
from kernel.topology import build
from nodes.node import Node, load_config

BASE = "config/cluster.json"
TMP = "config/_scale_tmp.json"


async def run_once(base_cfg, n_sites, n_managers, rate=40, n_jobs=150, burst=0.05):
    cfg = build(base_cfg, n_sites, n_managers, base_port=21000 + n_sites * 10 + n_managers)
    cfg["log_dir"] = f"logs/scale_{n_sites}s_{n_managers}m"
    with open(TMP, "w") as f:
        json.dump(cfg, f)
    names = [n["name"] for n in cfg["nodes"] if n["role"] != "client"]
    procs = launch(TMP, names)
    client = Node(cfg, "client")
    await client.start()
    try:
        await wait_registered(client, [n for n in names if n != "core"], timeout=25)
        await asyncio.sleep(1.0)
        submit = {}
        t0 = time.time()
        for i in range(n_jobs):
            jid = f"j{i}"
            submit[jid] = time.time()
            asyncio.create_task(client.send("core", client.msg("core", "SPAWN_REQUEST", {
                "job_id": jid, "cpu": 10, "mem": 32, "burst": burst, "prio": 5,
                "client": "client", "submit_ts": submit[jid]})))
            await asyncio.sleep(1.0 / rate)
        deadline = time.time() + 15
        while time.time() < deadline and len(client.done) < n_jobs:
            await asyncio.sleep(0.1)

        ok = [(v[0], v[0] - v[1]["submit_ts"]) for v in client.done.values() if v[1]["status"] == "ok"]
        t_end = max((x[0] for x in ok), default=t0 + 1)
        lat = sorted(x[1] for x in ok)

        core_stats = await client.send("core", client.msg("core", "STATS"), wait_reply=True, emulate=False)
        all_stats = []
        for n in names:
            r = await client.send(n, client.msg(n, "STATS"), wait_reply=True, emulate=False)
            all_stats.append(r.payload)
        total_coord = sum(s.get("coord_msgs", 0) for s in all_stats)

        return {
            "n_sites": n_sites, "n_managers": n_managers, "total_nodes": len(names) + 1,
            "jobs": n_jobs, "completed": len(ok),
            "throughput_jobs_s": round(len(ok) / (t_end - t0), 2),
            "latency_mean_ms": round(1000 * statistics.mean(lat), 1) if lat else None,
            "latency_p95_ms": round(1000 * lat[int(0.95 * (len(lat) - 1))], 1) if lat else None,
            "coord_msgs_total": total_coord,
            "coord_msgs_per_node": round(total_coord / (len(names) + 1), 2),
            "open_incidents_core": core_stats.payload.get("open_incidents", 0),
        }
    finally:
        await shutdown(client, names)
        client.tp.server.close()
        await client.tp.server.wait_closed()
        for p in procs:
            p.wait(timeout=5)


async def main():
    base_cfg = load_config(BASE)
    os.makedirs("results", exist_ok=True)
    configs = [(3, 2), (6, 2), (6, 4), (9, 4)]
    rows = []
    for n_sites, n_managers in configs:
        r = await run_once(base_cfg, n_sites, n_managers)
        rows.append(r)
        print(f"{n_sites} sites / {n_managers} managers ->",
              {k: r[k] for k in ("throughput_jobs_s", "latency_mean_ms", "coord_msgs_total", "coord_msgs_per_node")})
    with open("results/week3_scalability.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print("saved results/week3_scalability.csv")


if __name__ == "__main__":
    asyncio.run(main())
