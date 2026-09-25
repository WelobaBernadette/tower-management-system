"""One node implementation for every role: core, manager, site, client.

Each node = its own OS process with a message inbox (TCP), a resource manager,
a scheduler, a process manager and an event log.
"""
import asyncio
import json
import os
import random
import time

import psutil

from comms.message import Message
from comms.transport import Link, Transport
from kernel import placement
from kernel.process import ProcessManager
from kernel.resources import ResourceManager
from kernel.scheduler import Scheduler
from metrics.logger import EventLog


def load_config(path):
    with open(path) as f:
        return json.load(f)


class Node:
    def __init__(self, cfg, name):
        self.cfg = cfg
        self.spec = next(n for n in cfg["nodes"] if n["name"] == name)
        self.name, self.role = name, self.spec["role"]
        self.host, self.port = "127.0.0.1", self.spec["port"]
        self.log = EventLog(name, cfg.get("log_dir", "logs"))
        lk = cfg["link"]
        self.link = Link(lk["delay_ms"], lk["jitter_ms"], lk["loss"], cfg["seed"] + self.port)
        self.res = ResourceManager(self.spec["cpu"], self.spec["mem"])
        self.sched = Scheduler(cfg.get("policy", "rr"), cfg.get("quantum", 0.02))
        self.pm = ProcessManager(self)
        self.tp = Transport(self.host, self.port, self.handle, self.link)
        self.core_name = next(n["name"] for n in cfg["nodes"] if n["role"] == "core")
        self.registry = {}      # core: registered nodes
        self.rr = -1            # core: round-robin pointer
        self.reports = {}       # manager: latest site reports
        self.done = {}          # client: job_id -> (arrival time, payload)
        self.completed = 0
        self.samples = []       # (cpu %, rss MB)
        self.stop_event = asyncio.Event()

    # ---------- plumbing ----------
    def addr(self, name):
        n = next(x for x in self.cfg["nodes"] if x["name"] == name)
        return "127.0.0.1", n["port"]

    async def send(self, name, msg, **kw):
        host, port = self.addr(name)
        return await self.tp.send(host, port, msg, **kw)

    def msg(self, to, typ, payload=None):
        return Message(self.name, to, typ, payload or {})

    # ---------- lifecycle ----------
    async def start(self):
        await self.tp.start()
        asyncio.create_task(self.sched.run(self.on_job_done))
        asyncio.create_task(self.sampler())
        if self.role != "core":
            await self.register()
        if self.role in ("site", "manager"):
            asyncio.create_task(self.report_loop())
        if self.role == "site":
            self.pm.start_service("sensor", self.sensor_loop, 5, 16)
            self.pm.start_service("heartbeat", self.heartbeat_loop, 2, 8)
        self.log.event("node_started", role=self.role, cpu=self.res.cpu_total, mem=self.res.mem_total)

    async def register(self):
        payload = {"role": self.role, "cpu": self.res.cpu_total, "mem": self.res.mem_total}
        for _ in range(100):
            r = await self.send(self.core_name, self.msg(self.core_name, "REGISTER", payload),
                                wait_reply=True, emulate=False)
            if r is not None:
                return
            await asyncio.sleep(0.1)

    async def sampler(self):
        proc = psutil.Process()
        proc.cpu_percent()
        while True:
            await asyncio.sleep(0.5)
            self.samples.append((proc.cpu_percent(), proc.memory_info().rss / 1e6))

    async def report_loop(self):
        while True:
            await asyncio.sleep(0.5)
            load = self.sched.load()
            await self.send(self.core_name, self.msg(self.core_name, "RESOURCE_UPDATE",
                                                     {"load": load, "free": self.res.free()}))

    # ---------- site services ----------
    async def sensor_loop(self):
        rng = random.Random(self.cfg["seed"] + self.port)
        mgr = self.spec["manager"]
        while True:
            await asyncio.sleep(self.cfg.get("sensor_interval", 1.0))
            data = {"grid": rng.random() > 0.05, "battery": round(rng.uniform(20, 100), 1),
                    "fuel": round(rng.uniform(10, 100), 1), "link": rng.random() > 0.02}
            await self.send(mgr, self.msg(mgr, "SENSOR", data))

    async def heartbeat_loop(self):
        mgr = self.spec["manager"]
        while True:
            await asyncio.sleep(0.5)
            await self.send(mgr, self.msg(mgr, "HEARTBEAT"))

    # ---------- job completion ----------
    async def on_job_done(self, p):
        self.pm.finish(p)
        self.completed += 1
        m = p.meta
        if m.get("client"):
            await self.send(m["client"], self.msg(m["client"], "JOB_DONE", {
                "job_id": m["job_id"], "submit_ts": m["submit_ts"], "node": self.name, "status": "ok"}))

    # ---------- message handler ----------
    async def handle(self, m):
        t = m.type
        self.log.event("recv", type=t, src=m.sender, id=m.id, transit=time.time() - m.ts)
        if t == "REGISTER":
            self.registry[m.sender] = {**m.payload, "load": 0}
            self.log.event("registered", who=m.sender)
            return self.msg(m.sender, "ACK")
        if t == "REGISTRY":
            return self.msg(m.sender, "REGISTRY", {"nodes": sorted(self.registry)})
        if t == "RESOURCE_UPDATE" and m.sender in self.registry:
            self.registry[m.sender].update(m.payload)
        elif t == "SPAWN_REQUEST":  # core places the job
            workers = [n for n, i in self.registry.items() if i["role"] in ("site", "manager")]
            target = placement.choose(self.cfg.get("placement", "round_robin"), workers, self.registry, self)
            if target is None:
                return self.msg(m.sender, "NACK", {"reason": "no workers"})
            self.log.event("placed", job=m.payload["job_id"], target=target)
            asyncio.create_task(self.send(target, self.msg(target, "SPAWN", m.payload)))
            return self.msg(m.sender, "ACK", {"target": target})
        elif t == "SPAWN":  # any worker creates the process
            pl = m.payload
            p = self.pm.create_job("job", pl["cpu"], pl["mem"], pl["burst"], pl.get("prio", 5), pl)
            if p.state == "refused" and pl.get("client"):
                c = pl["client"]
                asyncio.create_task(self.send(c, self.msg(c, "JOB_DONE", {
                    "job_id": pl["job_id"], "submit_ts": pl["submit_ts"], "node": self.name, "status": "refused"})))
        elif t == "SENSOR":
            self.reports[m.sender] = {**m.payload, "seen": time.time()}
        elif t == "HEARTBEAT":
            self.reports.setdefault(m.sender, {})["seen"] = time.time()
        elif t == "JOB_DONE":
            self.done[m.payload["job_id"]] = (time.time(), m.payload)
        elif t == "STATS":
            cpu = [s[0] for s in self.samples] or [0]
            rss = [s[1] for s in self.samples] or [0]
            return self.msg(m.sender, "STATS", {
                "node": self.name, "role": self.role, "cpu_mean": sum(cpu) / len(cpu),
                "rss_max_mb": max(rss), "completed": self.completed, "refused": self.pm.refused,
                "link_sent": self.link.sent, "link_dropped": self.link.dropped, "free": self.res.free()})
        elif t == "SHUTDOWN":
            self.log.event("shutdown")
            self.stop_event.set()
        return self.msg(m.sender, "ACK") if t in ("SPAWN", "SENSOR", "HEARTBEAT") else None
