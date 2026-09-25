"""Week 1 demo: start the cluster, show registration, spawning, refusal and message flow.

    python cluster.py
"""
import asyncio
import json
import subprocess
import sys
import time

from comms.message import Message
from nodes.node import Node, load_config

CFG = "config/cluster.json"


def launch(cfg_path, names):
    return [subprocess.Popen([sys.executable, "-m", "nodes.run", "--config", cfg_path, "--name", n])
            for n in names]


async def wait_registered(client, expected, timeout=15):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = await client.send("core", client.msg("core", "REGISTRY"), wait_reply=True, emulate=False)
        if r and set(expected) <= set(r.payload["nodes"]):
            return r.payload["nodes"]
        await asyncio.sleep(0.2)
    raise RuntimeError("cluster did not come up")


async def shutdown(client, names):
    for n in names:
        await client.send(n, client.msg(n, "SHUTDOWN"), emulate=False)


async def demo():
    cfg = load_config(CFG)
    names = [n["name"] for n in cfg["nodes"] if n["role"] != "client"]
    procs = launch(CFG, names)
    client = Node(cfg, "client")
    await client.start()
    try:
        print("1) Nodes registering with core ...")
        print("   registered:", await wait_registered(client, [n for n in names if n != "core"]))

        print("2) Site processes (services) started on site nodes - see logs/site1.jsonl")

        print("3) Spawning 5 jobs through the core (placement =", cfg["placement"] + ")")
        for i in range(5):
            r = await client.send("core", client.msg("core", "SPAWN_REQUEST", {
                "job_id": f"demo{i}", "cpu": 10, "mem": 32, "burst": 0.1, "prio": 5,
                "client": "client", "submit_ts": time.time()}), wait_reply=True, emulate=False)
            print(f"   demo{i} placed on", r.payload.get("target"))
        await asyncio.sleep(1.0)
        print("   completed:", {k: v[1]["node"] for k, v in client.done.items()})

        print("4) Resource refusal: asking for more memory than any node has")
        await client.send("core", client.msg("core", "SPAWN_REQUEST", {
            "job_id": "toobig", "cpu": 10, "mem": 9999, "burst": 0.1, "client": "client",
            "submit_ts": time.time()}), wait_reply=True, emulate=False)
        await asyncio.sleep(0.5)
        print("   result:", client.done.get("toobig", (0, {}))[1].get("status"))

        print("5) Message flow: manager stats")
        r = await client.send("mgr1", client.msg("mgr1", "STATS"), wait_reply=True, emulate=False)
        print("  ", json.dumps(r.payload))
    finally:
        await shutdown(client, names)
        for p in procs:
            p.wait(timeout=5)


if __name__ == "__main__":
    asyncio.run(demo())
