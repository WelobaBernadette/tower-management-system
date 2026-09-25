"""Per-node scheduler. Policies: 'rr' (round robin with quantum) and 'priority'.

A node models one CPU: it runs one job slice at a time. Replace or extend the
policy here in later weeks (deadlines, priorities from incident scores, etc.).
"""
import asyncio
import time
from collections import deque


class Scheduler:
    def __init__(self, policy="rr", quantum=0.02):
        self.policy, self.quantum = policy, quantum
        self.ready = deque()
        self.current = None

    def add(self, p):
        p.state = "ready"
        self.ready.append(p)

    def pick(self):
        if not self.ready:
            return None
        if self.policy == "priority":
            p = min(self.ready, key=lambda x: (x.prio, x.created))
            self.ready.remove(p)
            return p
        return self.ready.popleft()

    def load(self):
        return len(self.ready) + (1 if self.current else 0)

    async def run(self, on_done):
        while True:
            p = self.pick()
            if p is None:
                await asyncio.sleep(0.002)
                continue
            self.current = p
            p.state = "running"
            if p.started is None:
                p.started = time.time()
            slice_ = min(self.quantum, p.remaining)
            await asyncio.sleep(slice_)  # simulated CPU burst
            p.remaining -= slice_
            self.current = None
            if p.remaining <= 1e-9:
                p.state = "terminated"
                await on_done(p)
            else:
                self.add(p)
