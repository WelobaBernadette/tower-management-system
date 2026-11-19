"""
Milestone 7: Recovery, Availability & MTTR
Author: Bathshua

Tests node crashes and calculates Availability (T_up / T_total) and MTTR.
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

BASE, TMP, LOGS = "config/cluster.json", "config/_week7_tmp.json", "logs/week7_bathshua"
T_FAULT, T_REPAIR, T_END = 5.0, 13.0, 24.0
PROBE_S, UP_AGE = 0.25, 0.5

async def ask(client, name, typ, payload=None, timeout=3.0):
    return await client.send(name, client.msg(name, typ, payload or {}), wait_reply=True, emulate=False, timeout=timeout)

class Cluster:
    def __init__(self, base_port=28200):
        self.cfg = build(load_config(BASE), 3, 2, base_port=base_port)
        self.cfg["log_dir"], self.cfg["sensor_interval"] = LOGS, 0.25
        self.cfg["log_append"], self.cfg["trace_faults"] = True, True
        self.cfg["ft"] = {"mode": "baseline", "miss_limit": 3, "detect_timeout": 1.5, "sync_interval": 0.5}
        self.names = [n["name"] for n in self.cfg["nodes"] if n["role"] != "client"]
        self.sites = [n["name"] for n in self.cfg["nodes"] if n["role"] == "site"]
        self.site_mgr = {n["name"]: n["manager"] for n in self.cfg["nodes"] if n["role"] == "site"}
        self.mode = "baseline"

    async def __aenter__(self):
        shutil.rmtree(LOGS, ignore_errors=True)
        with open(TMP, "w") as f:
            json.dump(self.cfg, f)
        self.procs = launch(TMP, self.names)
        self.client = Node(self.cfg, "client")
        await self.client.start()
        await wait_registered(self.client, [n for n in self.names if n != "core"], timeout=40)
        t0 = time.time()
        while time.time() - t0 < 40:
            sts = [await ask(self.client, m, "STATS", timeout=1.0) for m in ("mgr1", "mgr2")]
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

    def kill(self, name):
        p = self.procs[self.names.index(name)]
        p.kill()
        p.wait()

    def restart(self, name):
        self.procs[self.names.index(name)] = subprocess.Popen(
            [sys.executable, "-m", "nodes.run", "--config", TMP, "--name", name],
            env={**os.environ, "FT_MODE": self.mode})

    async def configure(self, mode, **params):
        self.mode = mode
        payload = {"mode": mode, "reset": True, "miss_limit": 3, "detect_timeout": 1.5, "sync_interval": 0.5, "ack_timeout": 0.4, "rereg_interval": 1.0, "site_timeout": 3.0, **params}
        await asyncio.gather(*[ask(self.client, n, "FT_CONFIG", payload) for n in self.names])

async def probe(cl, series, stop):
    while not stop.is_set():
        t = time.time()
        rs = await asyncio.gather(*[ask(cl.client, s, "STATS", timeout=0.4) for s in cl.sites],
                                  ask(cl.client, "core", "REGISTRY", timeout=0.4))
        for s, r in zip(cl.sites, rs[:-1]):
            series[s].append((t, r.payload["ft_ok_age"] if r else 99.0))
        core = rs[-1]
        series["_core"].append((t, core is not None, core.payload["nodes"] if core else []))
        await asyncio.sleep(max(0.0, PROBE_S - (time.time() - t)))

def scan(points):
    up_n = tot = 0
    outages, down_since, last_t = [], None, None
    for t, up in points:
        tot += 1
        up_n += up
        if not up and down_since is None:
            down_since = t
        if up and down_since is not None:
            outages.append(t - down_since)
            down_since = None
        last_t = t
    if down_since is not None:
        outages.append(last_t - down_since)
    return up_n, tot, outages

async def run_one(cl, scen, mode):
    await cl.configure(mode)
    await asyncio.sleep(3.0)
    series, stop = {s: [] for s in cl.sites}, asyncio.Event()
    series["_core"] = []
    pt = asyncio.create_task(probe(cl, series, stop))
    
    await asyncio.sleep(T_FAULT)
    if scen == "manager_crash":
        cl.kill("mgr1")
    elif scen == "core_crash":
        cl.kill("core")
    elif scen == "site_crash":
        cl.kill("site1")
        
    await asyncio.sleep(T_REPAIR - T_FAULT)
    
    if scen == "manager_crash":
        cl.restart("mgr1")
    elif scen == "core_crash":
        cl.restart("core")
    elif scen == "site_crash":
        cl.restart("site1")
        
    await asyncio.sleep(T_END - T_REPAIR)
    stop.set()
    await pt
    await asyncio.sleep(1.0)

    # Analyse Availability & MTTR
    if scen == "core_crash":
        watch, basis = ["_core"], "core"
        pts = {"_core": [(t, up) for t, up, _ in series["_core"]]}
    elif scen == "site_crash":
        watch, basis = ["site1"], "site1"
        pts = {"site1": [(t, age < UP_AGE) for t, age in series["site1"]]}
    else:
        watch = [s for s in cl.sites if cl.site_mgr[s] == "mgr1"]
        basis = "sites of mgr1"
        pts = {s: [(t, age < UP_AGE) for t, age in series[s]] for s in watch}
        
    up_n = tot = 0
    outages = []
    for k in watch:
        u, n, o = scan(pts[k])
        up_n, tot, outages = up_n + u, tot + n, outages + o

    return {
        "scenario": scen, "mode": mode, "avail_basis": basis,
        "availability": round(up_n / tot, 4) if tot else "",
        "outages": len(outages), "mttr_s": round(statistics.mean(outages), 2) if outages else 0.0,
        "downtime_s": round(sum(outages) / len(watch), 2)
    }

async def main():
    acquire_lock()
    write_env({"experiment": "week7_bathshua", "timeline_s": f"{T_FAULT}/{T_REPAIR}/{T_END}"})
    scenarios = ("manager_crash", "core_crash", "site_crash")
    rows = []
    
    async with Cluster() as cl:
        for scen in scenarios:
            for mode in ("baseline", "proposed"):
                r = await run_one(cl, scen, mode)
                rows.append(r)
                print(f"{scen:15} {mode:9} Availability: {r['availability']}  MTTR: {r['mttr_s']}s  Downtime: {r['downtime_s']}s")

    os.makedirs("results", exist_ok=True)
    with open("results/week7_recovery_results.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print("saved results/week7_recovery_results.csv")

if __name__ == "__main__":
    asyncio.run(main())