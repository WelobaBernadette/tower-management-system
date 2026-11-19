"""
Milestone 8: Cost, Decision Matrix & Stress Testing
Author: Tebby

Calculates cost model, runs the decision matrix, and tests system stress at high loads.
"""
import asyncio
import csv
import json
import os
import shutil
import statistics
import subprocess
import sys
import time

from cluster import launch, shutdown, wait_registered
from experiments._common import acquire_lock, write_env
from kernel.topology import build
from nodes.node import Node, load_config

TMP, LOGS = "config/_week8_tmp.json", "logs/week8_tebby"
MODELS = {"host_based": ("core", None), "server_based": ("manager", None), "workstation": ("site", None), "processor_pool": ("pool", None), "integrated": ("integrated", "proposed")}
FT_CFG = {"miss_limit": 3, "detect_timeout": 1.5, "sync_interval": 0.5}
WARMUP, MATCH_S = 3.0, 3.0
COST = {"site_terminal": 1.0, "site_workstation": 2.0, "manager": 3.0, "core_base": 5.0, "core_per_msg_s": 0.05, "bandwidth_per_msg_s": 0.02}
COST_VARIANTS = {"assumed costs": {}, "cheap workstations": {"site_workstation": 1.2}, "expensive workstations": {"site_workstation": 4.0}, "expensive managers": {"manager": 6.0}, "expensive bandwidth": {"bandwidth_per_msg_s": 0.1}, "expensive core": {"core_base": 15.0}}
PROFILES = {"equal weights": dict(latency=1, core_load=1, network=1, cost=1, reliability=1, throughput=1), "telecom": dict(latency=.25, core_load=.10, network=.10, cost=.10, reliability=.35, throughput=.10), "cost-driven": dict(latency=.10, core_load=.10, network=.15, cost=.45, reliability=.10, throughput=.10), "performance-driven": dict(latency=.30, core_load=.15, network=.10, cost=.05, reliability=.10, throughput=.30)}
CRITERIA = ("latency", "core_load", "network", "cost", "reliability", "throughput")

async def ask(client, name, typ, payload=None, timeout=3.0):
    return await client.send(name, client.msg(name, typ, payload or {}), wait_reply=True, emulate=False, timeout=timeout)

class Cluster:
    def __init__(self, model, n_sites, interval=0.3, base_port=29400):
        tier, ft = MODELS[model]
        self.cfg = build(load_config("config/cluster.json"), n_sites, 2, base_port=base_port)
        self.cfg.update({"log_dir": LOGS, "sensor_interval": interval, "trace_faults": True, "log_append": True, "correlation_tier": tier})
        if ft: self.cfg["ft"] = {"mode": ft, **FT_CFG}
        self.model = model
        self.names = [n["name"] for n in self.cfg["nodes"] if n["role"] != "client"]
        self.sites = [n["name"] for n in self.cfg["nodes"] if n["role"] == "site"]

    async def __aenter__(self):
        shutil.rmtree(LOGS, ignore_errors=True)
        with open(TMP, "w") as f: json.dump(self.cfg, f)
        self.procs = launch(TMP, self.names)
        self.client = Node(self.cfg, "client")
        await self.client.start()
        await wait_registered(self.client, [n for n in self.names if n != "core"], timeout=60)
        return self

    async def __aexit__(self, *exc):
        await shutdown(self.client, self.names)
        self.client.tp.server.close()
        await self.client.tp.server.wait_closed()
        for p in self.procs:
            try: p.wait(timeout=5)
            except Exception: p.kill()

    async def all_stats(self):
        rs = await asyncio.gather(*[ask(self.client, n, "STATS", timeout=2.0) for n in self.names])
        return {n: r.payload for n, r in zip(self.names, rs) if r}

def events(node, kinds):
    out = []
    try:
        with open(os.path.join(LOGS, f"{node}.jsonl")) as f:
            for line in f:
                try:
                    e = json.loads(line)
                    if e.get("event") in kinds: out.append(e)
                except ValueError: pass
    except OSError: pass
    return out

def detections(cl, lo, hi, match_s):
    faults = sorted((e["ts"], e["fid"]) for s in cl.sites for e in events(s, {"sensor_fault"}) if lo <= e["ts"] < hi)
    first = {}
    for e in sorted(events("core", {"incident_escalated", "correlated_incident"}), key=lambda e: e["ts"]):
        if e.get("fid") is not None and e["fid"] not in first: first[e["fid"]] = e["ts"]
    return [None if first.get(fid) is None or first.get(fid) < fts or (match_s is not None and first.get(fid) - fts > match_s) else first.get(fid) - fts for fts, fid in faults]

async def performance(model, n_sites, duration, interval=0.3):
    async with Cluster(model, n_sites, interval) as cl:
        await asyncio.sleep(WARMUP)
        a = await cl.all_stats()
        t0 = time.time()
        await asyncio.sleep(duration)
        t1 = time.time()
        b = await cl.all_stats()
        await asyncio.sleep(0.5)
        lat = detections(cl, t0, t1 - MATCH_S, MATCH_S)
        got = [x for x in lat if x is not None]
        eff = max(duration - MATCH_S, 1.0)
        recv = lambda node, types=None: sum(1 for e in events(node, {"recv"}) if t0 <= e["ts"] < t1 and (types is None or e.get("type") in types)) / duration
        DATA_CORE = {"SENSOR", "CORRELATED_INCIDENT", "INCIDENT_RESOLVED"}
        role_of = {x["name"]: x["role"] for x in cl.cfg["nodes"]}
        by = lambda role: [n for n in cl.names if role_of[n] == role]
        net = sum(b[n]["link_sent"] - a.get(n, {}).get("link_sent", 0) for n in b) / duration
        return {"model": model, "sites": n_sites, "interval_s": interval, "detect_ratio": round(len(got)/len(lat), 3) if lat else "", "core_data_msgs_s": round(recv("core", DATA_CORE), 1), "net_msgs_s": round(net, 1), "throughput_inc_s": round(len(got)/eff, 2)}

def cost(model, n_sites, row, c=COST):
    sites = n_sites * (c["site_workstation"] if model in ("workstation", "integrated") else c["site_terminal"])
    managers = 0 if model in ("host_based", "workstation") else 2 * c["manager"]
    load = c["core_per_msg_s"] * row["core_data_msgs_s"]
    bandwidth = c["bandwidth_per_msg_s"] * row["net_msgs_s"]
    return {"model": model, "sites": n_sites, "hardware": round(sites + managers + c["core_base"], 2), "core_load_cost": round(load, 2), "bandwidth_cost": round(bandwidth, 2), "total_cost": round(sites + managers + c["core_base"] + load + bandwidth, 2)}

def raw_table(perf, stress, n_sites, cvar):
    c = {**COST, **cvar}
    raw = {}
    for m in MODELS:
        p = next((r for r in perf if r["model"] == m and r["sites"] == n_sites), None)
        if p is None: return None
        st = [r for r in stress if r["model"] == m]
        top = max(st, key=lambda r: 1 / r["interval_s"]) if st else None
        raw[m] = {"latency": p["detect_ratio"], "core_load": p["core_data_msgs_s"], "network": p["net_msgs_s"], "cost": cost(m, n_sites, p, c)["total_cost"], "reliability": p["detect_ratio"], "throughput": top["detect_ratio"] if top and top["detect_ratio"] != "" else None}
    return raw

def score(raw, weights):
    crit = [k for k in CRITERIA if all(raw[m][k] not in (None, "") for m in raw)]
    wsum = sum(weights.get(k, 0) for k in crit) or 1.0
    sc = {m: 0.0 for m in raw}
    for k in crit:
        vals = [raw[m][k] for m in raw]
        lo, hi = min(vals), max(vals)
        for m in raw:
            x = 0.5 if hi == lo else (raw[m][k] - lo) / (hi - lo)
            sc[m] += weights.get(k, 0) * (x if k in ("reliability", "throughput", "latency") else 1 - x)
    return {m: round(sc[m] / wsum, 3) for m in sc}, crit

async def main():
    acquire_lock()
    write_env({"experiment": "week8_tebby"})
    stress, perf = [], []
    n_ref = 9

    # Run a short perf test to get data for the cost matrix
    for model in MODELS:
        r = await performance(model, n_ref, 10.0)
        perf.append(r)

    # Stress Test
    for model in MODELS:
        for interval in (0.3, 0.1, 0.05):
            r = await performance(model, n_ref, 10.0, interval)
            r["offered_readings_s"] = round(n_ref / interval, 1)
            stress.append(r)
            print(f"stress {model:15} {r['offered_readings_s']:>6} readings/s  detected {r['detect_ratio']}  core cpu (n/a)")

    # Decision Matrix
    rows = []
    for name, w in PROFILES.items():
        raw = raw_table(perf, stress, n_ref, {})
        if raw:
            sc, _ = score(raw, w)
            ranked = sorted(sc, key=lambda m: -sc[m])
            for i, m in enumerate(ranked):
                rows.append({"scenario": name, "model": m, "score": sc[m], "rank": i + 1})
                print(f"Decision {name:20} > {m} ({sc[m]})")

    os.makedirs("results", exist_ok=True)
    with open("results/week8_stress.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(stress[0]))
        w.writeheader()
        w.writerows(stress)
    with open("results/week8_decision.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print("saved results/week8_stress.csv and week8_decision.csv")

if __name__ == "__main__":
    asyncio.run(main())