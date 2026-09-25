"""Start one node:  python -m nodes.run --config config/cluster.json --name site1"""
import argparse
import asyncio

from nodes.node import Node, load_config


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/cluster.json")
    ap.add_argument("--name", required=True)
    a = ap.parse_args()
    node = Node(load_config(a.config), a.name)
    await node.start()
    await node.stop_event.wait()
    await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main())
