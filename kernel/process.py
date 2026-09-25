"""Process model and process manager.

States: new -> ready -> running -> terminated (or refused if resources unavailable).
Two kinds: jobs (scheduled bursts of work) and services (long-running loops).
"""
import asyncio
import itertools
import time


class Process:
    _ids = itertools.count(1)

    def __init__(self, name, cpu, mem, burst=0.0, prio=5, meta=None):
        self.pid = next(Process._ids)
        self.name, self.cpu, self.mem = name, cpu, mem
        self.burst = self.remaining = burst
        self.prio = prio
        self.state = "new"
        self.created = time.time()
        self.started = None
        self.meta = meta or {}
        self.task = None


class ProcessManager:
    def __init__(self, node):
        self.node = node
        self.table = {}
        self.refused = 0

    def create_job(self, name, cpu, mem, burst, prio=5, meta=None):
        p = Process(name, cpu, mem, burst, prio, meta)
        if not self.node.res.allocate(cpu, mem):
            p.state = "refused"
            self.refused += 1
            self.node.log.event("proc_refused", pid=p.pid, name=name, cpu=cpu, mem=mem,
                                free=self.node.res.free())
            return p
        self.table[p.pid] = p
        self.node.log.event("proc_created", pid=p.pid, name=name, cpu=cpu, mem=mem, burst=burst)
        self.node.sched.add(p)
        return p

    def start_service(self, name, coro_fn, cpu, mem):
        p = Process(name, cpu, mem)
        if not self.node.res.allocate(cpu, mem):
            p.state = "refused"
            self.node.log.event("proc_refused", pid=p.pid, name=name)
            return p
        p.state = "running"
        p.task = asyncio.create_task(coro_fn())
        self.table[p.pid] = p
        self.node.log.event("service_started", pid=p.pid, name=name)
        return p

    def finish(self, p):
        self.node.res.release(p.cpu, p.mem)
        self.table.pop(p.pid, None)
        self.node.log.event("proc_terminated", pid=p.pid, name=p.name,
                            waited=(p.started or time.time()) - p.created,
                            total=time.time() - p.created)
