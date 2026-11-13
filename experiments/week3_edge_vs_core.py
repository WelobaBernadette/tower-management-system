"""Milestone 3, part 3: edge vs core trade-off for fault correlation.

Hypothesis: running correlation at the core removes one network hop from
detection (site -> core directly, instead of site -> manager -> core), so it
should detect faster, but every sensor reading now lands on the core instead
of only state changes, so it should cost the core far more inbound traffic.
The gap in both should widen as link delay grows.

Independent variables : correlation_tier (edge / core), link delay (ms)
Dependent variables   : detection latency at the core (ms), inbound messages
                        at the core per second
Controlled variables  : 3 sites, 1 manager, seed 42, sensor_interval 0.3 s
                        (5x normal, a stress setting so enough faults occur
                        in a short run), 15 s per run

    python3 -m experiments.week3_edge_vs_core
Writes results/week3_edge_vs_core.csv
"""
import asyncio
import json
import os
import statistics
import time

from cluster import launch, shutdown, wait_registered
from kernel.topology import build
from nodes.node import Node, load_config

BASE = "config/cluster.json"
TMP = "config/_edgecore_tmp.json"
SITES, MANAGERS = 3, 1
DURATION = 15
SENSOR_INTERVAL = 0.3
DELAYS_MS = (5, 40)


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


def ms(vals):
    return round(1000 * statistics.mean(vals), 2) if vals else None


async def run_once(base_cfg, tier, delay_ms):
    cfg = build(base_cfg, SITES, MANAGERS, base_port=26000 + delay_ms)
    cfg["correlation_tier"] = tier
    cfg["sensor_interval"] = SENSOR_INTERVAL
    cfg["link"]["delay_ms"] = delay_ms
    cfg["log_dir"] = f"logs/edgecore_{tier}_{delay_ms}ms"
    with open(TMP, "w") as f:
        json.dump(cfg, f)
    names = [n["name"] for n in cfg["nodes"] if n["role"] != "client"]
    procs = launch(TMP, names)
    client = Node(cfg, "client")
    await client.start()
    try:
        await wait_registered(client, [n for n in names if n != "core"], timeout=30)
        print(f"  {tier:5} tier, {delay_ms:3} ms link: running {DURATION}s ...")
        await asyncio.sleep(DURATION)
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
    core_log = read(f"{d}/core.jsonl")
    mgr_log = read(f"{d}/mgr1.jsonl")
    correlator_log = mgr_log if tier == "edge" else core_log

    opens = [e for e in correlator_log if e.get("event") == "correlated_incident"]
    sensing_hop = ms([e["transit"] for e in correlator_log
                      if e.get("event") == "recv" and e.get("type") == "SENSOR"])
    escalation_hop = None
    if tier == "edge":
        escalation_hop = ms([e["transit"] for e in core_log
                             if e.get("event") == "recv" and e.get("type") == "CORRELATED_INCIDENT"])
        core_inbound = sum(1 for e in core_log
                           if e.get("event") == "recv" and e.get("type") in ("CORRELATED_INCIDENT", "INCIDENT_RESOLVED"))
    else:
        core_inbound = sum(1 for e in core_log if e.get("event") == "recv" and e.get("type") == "SENSOR")
    total_detect = round((sensing_hop or 0) + (escalation_hop or 0), 2) if sensing_hop is not None else None

    return {"tier": tier, "link_delay_ms": delay_ms, "opens": len(opens),
            "sensing_hop_ms": sensing_hop, "escalation_hop_ms": escalation_hop,
            "total_detect_ms": total_detect, "core_inbound_msgs": core_inbound,
            "core_inbound_per_s": round(core_inbound / DURATION, 2)}


async def main():
    import csv
    base_cfg = load_config(BASE)
    os.makedirs("results", exist_ok=True)
    rows = []
    for delay in DELAYS_MS:
        for tier in ("edge", "core"):
            rows.append(await run_once(base_cfg, tier, delay))
    with open("results/week3_edge_vs_core.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    print(f"\n{'tier':6} {'delay':6} {'opens':6} {'sense hop':10} {'escalate hop':13} {'total detect':13} {'core msgs':10} {'core/s':7}")
    for r in rows:
        print(f"{r['tier']:6} {r['link_delay_ms']:>4}ms {r['opens']:>6} "
              f"{str(r['sensing_hop_ms']) + 'ms':>10} {str(r['escalation_hop_ms']) + 'ms':>13} "
              f"{str(r['total_detect_ms']) + 'ms':>13} {r['core_inbound_msgs']:>10} {r['core_inbound_per_s']:>7}")
    print("\nsaved results/week3_edge_vs_core.csv")
    print("Read together: edge tier adds a second hop to detection (site->manager->core) but the core\n"
          "only hears about OPEN/RESOLVED changes; core tier detects in one hop but the core hears\n"
          "every sensor reading. If the pattern holds, the detection gap should roughly track link\n"
          "delay (edge ~= 2x delay, core ~= 1x delay), while core_inbound_per_s should be far higher\n"
          "for the core tier, since it scales with the sensor rate, not the fault rate.")


if __name__ == "__main__":
    asyncio.run(main())
