"""
Milestone 8: Performance & Reliability of Models
Author: Kelvin

Tests 5 models across 3, 6, and 9 sites for Latency, Throughput, and CPU Utilisation.
Also tests reliability by killing mgr1 and core.
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

TMP, LOGS = "config/_week8_tmp.json", "logs/week8_kelvin"
MODELS = {"host_based": ("core", None), "server_based": ("manager", None), "workstation": ("site", None), "processor_pool": ("pool", None), "integrated": ("integrated", "proposed")}
FT_CFG = {"miss_limit": 3, "detect_timeout": 1.5, "sync_interval": 0.5}
WARMUP, MATCH_S = 3.0, 3.0

async def ask(client, name, typ, payload=None, timeout=3.0):
    return await client.send(name, client.msg(name, typ, payload or {}), wait_reply=True, emulate=False, timeout=timeout)

class Cluster:
    def __init__(self, model, n_sites, interval=0.3, base_port=29300):
        tier, ft = MODELS[model]
        self.cfg = build(load_config("config/cluster.json"), n_sites, 2, base_port=base_port)
        self.cfg.update({"log_dir": LOGS, "sensor_interval": interval, "trace_faults": True, "log_append": True, "correlation_tier": tier})
        if ft:
            self.cfg["ft"] = {"mode": ft, **FT_CFG}
        self.model = model
        self.names = [n["name"] for n in self.cfg["nodes"] if n["role"] != "client"]
        self.sites = [n["name"] for n in self.cfg["nodes"] if n["role"] == "site"]

    async def __aenter__(self):
        shutil.rmtree(LOGS, ignore_errors=True)
        with open(TMP, "w") as f:
            json.dump(self.cfg, f)
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
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    def kill(self, name):
        p = self.procs[self.names.index(name)]
        p.kill()
        p.wait()

    def restart(self, name):
        self.procs[self.names.index(name)] = subprocess.Popen(
            [sys.executable, "-m", "nodes.run", "--config", TMP, "--name", name])

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
                    if e.get("event") in kinds:
                        out.append(e)
                except ValueError:
                    pass
    except OSError:
        pass
    return out

def detections(cl, lo, hi, match_s):
    faults = sorted((e["ts"], e["fid"]) for s in cl.sites for e in events(s, {"sensor_fault"}) if lo <= e["ts"] < hi)
    first = {}
    for e in sorted(events("core", {"incident_escalated", "correlated_incident"}), key=lambda e: e["ts"]):
        if e.get("fid") is not None and e["fid"] not in first:
            first[e["fid"]] = e["ts"]
    out = []
    for fts, fid in faults:
        c = first.get(fid)
        out.append(None if c is None or c < fts or (match_s is not None and c - fts > match_s) else c - fts)
    return out

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
        recv = lambda node, types=None: sum(1 for e in events(node, {"recv"})
                                            if t0 <= e["ts"] < t1 and (types is None or e.get("type") in types)) / duration
        DATA_CORE = {"SENSOR", "CORRELATED_INCIDENT", "INCIDENT_RESOLVED"}
        DATA_MGR = {"SENSOR", "POOL_SENSOR"}
        role_of = {x["name"]: x["role"] for x in cl.cfg["nodes"]}
        by = lambda role: [n for n in cl.names if role_of[n] == role]
        cpu = lambda role: round(statistics.mean(b[n]["cpu_mean"] for n in by(role) if n in b), 1)
        mem = lambda role: round(statistics.mean(b[n]["rss_max_mb"] for n in by(role) if n in b), 1)
        net = sum(b[n]["link_sent"] - a.get(n, {}).get("link_sent", 0) for n in b) / duration
        mgrs = by("manager")
        return {"model": model, "sites": n_sites, "faults": len(lat), "detected": len(got),
                "detect_ratio": round(len(got) / len(lat), 3) if lat else "",
                "detect_mean_ms": round(1000 * statistics.mean(got), 1) if got else "",
                "throughput_inc_s": round(len(got) / eff, 2),
                "core_in_msgs_s": round(recv("core"), 1), "core_data_msgs_s": round(recv("core", DATA_CORE), 1),
                "net_msgs_s": round(net, 1), "cpu_site_pct": cpu("site"), "cpu_mgr_pct": cpu("manager"), "cpu_core_pct": cpu("core"),
                "mem_core_mb": mem("core")}

async def reliability(model, target):
    t_fault, t_repair, t_end = 6.0, 14.0, 30.0
    async with Cluster(model, 6) as cl:
        await asyncio.sleep(WARMUP)
        await asyncio.sleep(t_fault)
        ts_f = time.time()
        cl.kill(target)
        await asyncio.sleep(t_repair - t_fault)
        ts_r = time.time()
        cl.restart(target)
        await asyncio.sleep(t_end - t_repair)
        await asyncio.sleep(0.5)
        lat = detections(cl, ts_f, ts_r, None)
        got = [x for x in lat if x is not None]
        return {"model": model, "killed": target, "faults_in_outage": len(lat), "detected": len(got),
                "detect_ratio": round(len(got) / len(lat), 3) if lat else ""}

async def main():
    acquire_lock()
    write_env({"experiment": "week8_kelvin"})
    perf, rel = [], []
    sizes = (3, 6, 9)
    duration = 15.0

    for n in sizes:
        for model in MODELS:
            r = await performance(model, n, duration)
            perf.append(r)
            print(f"{model:15} {n} sites  detect {r['detect_mean_ms']} ms  thr {r['throughput_inc_s']}/s  core cpu {r['cpu_core_pct']}%")

    for model in MODELS:
        for target in ("mgr1", "core"):
            r = await reliability(model, target)
            rel.append(r)
            print(f"reliability {model:15} kill {target:5} -> detected {r['detected']}/{r['faults_in_outage']} ({r['detect_ratio']})")

    os.makedirs("results", exist_ok=True)
    with open("results/week8_performance.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(perf[0]))
        w.writeheader()
        w.writerows(perf)
    with open("results/week8_reliability.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rel[0]))
        w.writeheader()
        w.writerows(rel)
    print("saved results/week8_performance.csv and week8_reliability.csv")

if __name__ == "__main__":
    asyncio.run(main())