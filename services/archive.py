"""Archive service (core -> cloud): periodic snapshots for long-term storage.

Moved out of nodes/node.py in Milestone 3 with no change in behaviour.
  run()         core side: every `archive_interval` seconds send an ARCHIVE snapshot
  on_archive()  cloud side: store a received snapshot
"""
import asyncio
import time


class ArchiveService:
    def __init__(self, node):
        self.node = node

    async def run(self):
        node = self.node
        cloud = next((x["name"] for x in node.cfg["nodes"] if x["role"] == "cloud"), None)
        if not cloud:
            return
        interval = node.cfg.get("archive_interval", 2.0)
        while True:
            await asyncio.sleep(interval)
            snapshot = {"ts": time.time(), "nodes": len(node.registry),
                        "open_incidents": len(node.incidents), "jobs_completed": node.completed}
            await node.send(cloud, node.msg(cloud, "ARCHIVE", snapshot))
            node.coord_msgs += 1

    def on_archive(self, payload):
        self.node.archive.append(payload)
        self.node.log.event("archived", **payload)
