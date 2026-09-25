"""Append-only JSON-lines event log, one file per node. Feeds every Week 2+ measurement."""
import json
import os
import time


class EventLog:
    def __init__(self, node, log_dir="logs"):
        os.makedirs(log_dir, exist_ok=True)
        self.node = node
        self.f = open(os.path.join(log_dir, f"{node}.jsonl"), "w", buffering=1)

    def event(self, kind, **fields):
        self.f.write(json.dumps({"ts": time.time(), "node": self.node, "event": kind, **fields}) + "\n")
