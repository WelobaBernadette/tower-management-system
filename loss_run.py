import asyncio
from experiments.week2_load import run_once, load_config
print(asyncio.run(run_once(load_config('config/cluster.json'), 'round_robin', 60, loss=0.05)))
