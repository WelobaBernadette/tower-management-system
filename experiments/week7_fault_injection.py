"""
Milestone 7: Fault Injection & Byzantine Analysis
Author: Bernadette

Tests communication failures (omission, partition), service failures, and Byzantine faults.
Compares baseline vs proposed (outbox delivery).
"""
import asyncio
import csv
import json
import os
import shutil
import subprocess
import sys
import time

from cluster import launch, shutdown, wait_registered
from experiments._common import acquire_lock, write_env
from kernel.topology import build
from nodes.node import Node, load_config

BASE, TMP, LOGS = "config/cluster.json", "config/_week7_tmp.json", "logs/week7_bernadette"
T_FAULT, T_REPAIR, T_END = 5.0, 13.0, 24.0

async def ask(client, name, typ, payload=None, timeout=3.0):
    return await client.send(name, client.msg(name, typ, payload or {}), wait_reply=True, emulate=False, timeout=timeout)

class Cluster:
    def __init__(self, base_port=28100):
        self.cfg = build(load_config(BASE), 3, 2, base_port=base_port)
        self.cfg["log_dir"], self.cfg["sensor_interval"] = LOGS, 0.25
        self.cfg["log_append"], self.cfg["trace_faults"] = True, True
        self.cfg["ft"] = {"mode": "baseline", "miss_limit": 3, "detect_timeout": 1.5, "sync_interval": 0.5}
        self.names = [n["name"] for n in self.cfg["nodes"] if n["role"] != "client"]
        self.sites = [n["name"] for n in self.cfg["nodes"] if n["role"] == "site"]
        self.mode = "baseline"

    async def __aenter__(self):
        shutil.rmtree(LOGS, ignore_errors=True)
        with open(TMP, "w") as f:
            json.dump(self.cfg, f)
        self.procs = launch(TMP, self.names)
        self.client = Node(self.cfg, "client")
        await self.client.start()
        await wait_registered(self.client, [n for n in self.names if n != "core"], timeout=40)
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

    async def inject(self, name, **payload):
        return await ask(self.client, name, "FT_INJECT", payload)

    async def configure(self, mode, **params):
        self.mode = mode
        payload = {"mode": mode, "reset": True, "miss_limit": 3, "detect_timeout": 1.5, "sync_interval": 0.5, "ack_timeout": 0.4, "rereg_interval": 1.0, "site_timeout": 3.0, **params}
        await asyncio.gather(*[ask(self.client, n, "FT_CONFIG", payload) for n in self.names])

def read_events(node, kind):
    out = []
    try:
        with open(os.path.join(LOGS, f"{node}.jsonl")) as f:
            for line in f:
                try:
                    e = json.loads(line)
                    if e.get("event") == kind:
                        out.append(e)
                except ValueError:
                    pass
    except OSError:
        pass
    return out

async def run_one(cl, scen, mode):
    await cl.configure(mode)
    await asyncio.sleep(3.0)
    t0 = time.time()
    fault = t0 + T_FAULT
    await asyncio.sleep(T_FAULT)
    
    if scen == "omission":
        await cl.inject("mgr1", kind="omission", loss=0.7, seconds=T_REPAIR - T_FAULT)
    elif scen == "partition":
        await cl.inject("mgr1", kind="omission", loss=1.0, seconds=T_REPAIR - T_FAULT)
    elif scen == "service_failure":
        await cl.inject("mgr1", kind="service_stop", service="correlation")
    elif scen == "byzantine":
        await cl.inject("mgr1", kind="byzantine", seconds=T_REPAIR - T_FAULT)
        
    await asyncio.sleep(T_REPAIR - T_FAULT)
    
    if scen == "service_failure":
        await cl.inject("mgr1", kind="service_start", service="correlation")
        
    await asyncio.sleep(T_END - T_REPAIR)
    await asyncio.sleep(1.0)
    
    # Analyse delivery
    gen, bogus_gen = {}, 0
    for m in ("mgr1",):
        for e in read_events(m, "ft_escalate"):
            if fault <= e["ts"] <= T_REPAIR:
                if e.get("bogus"):
                    bogus_gen += 1
                else:
                    gen[e["eid"]] = e["ts"]
    rec = [e for e in read_events("core", "ft_receive") if e["ts"] >= t0]
    recv = {e["eid"]: e["delay"] for e in rec if e["ts"] >= fault and not e.get("bogus")}
    bogus_acc = sum(1 for e in rec if e["ts"] >= fault and e.get("bogus"))
    
    delivered = [recv[k] for k in gen if k in recv]
    
    return {
        "scenario": scen, "mode": mode, "esc_generated": len(gen), "esc_delivered": len(delivered),
        "delivery_ratio": round(len(delivered) / len(gen), 3) if gen else "",
        "bogus_gen": bogus_gen, "bogus_accepted": bogus_acc
    }

async def main():
    acquire_lock()
    write_env({"experiment": "week7_bernadette", "timeline_s": f"{T_FAULT}/{T_REPAIR}/{T_END}"})
    scenarios = ("omission", "partition", "service_failure", "byzantine")
    rows = []
    
    async with Cluster() as cl:
        for scen in scenarios:
            for mode in ("baseline", "proposed"):
                r = await run_one(cl, scen, mode)
                rows.append(r)
                print(f"{scen:15} {mode:9} delivery: {r['delivery_ratio']} ({r['esc_delivered']}/{r['esc_generated']}) bogus: {r['bogus_accepted']}/{r['bogus_gen']}")

    os.makedirs("results", exist_ok=True)
    with open("results/week7_injection_results.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print("saved results/week7_injection_results.csv")

if __name__ == "__main__":
    asyncio.run(main())