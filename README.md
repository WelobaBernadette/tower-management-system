# Tower-Site Fault Management: Milestones 1 and 2 starter

Python 3.10+ (tested on 3.12), one dependency (psutil). Runs on Windows via WSL2 Ubuntu, or natively.

## Setup (WSL2 Ubuntu)
    sudo apt update && sudo apt install -y python3 python3-venv git
    cd ~ && unzip tower_fm_starter.zip && cd tower_fm
    python3 -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt

## Milestone 1 demo
    python cluster.py

Shows: nodes registering with the core, site services (sensor, heartbeat) started as processes,
jobs placed and scheduled, a resource refusal, and message flow. Per-node event logs are in logs/.

## Milestone 2 experiment
    python -m experiments.week2_load

Runs 3 placement policies x 3 request rates, writes results/week2_results.csv
(throughput, mean and p95 latency, jitter, packet loss, CPU, memory, jobs per node).

## Layout
    comms/        message format, TCP transport, seeded link emulator (delay, jitter, loss)
    kernel/       resource manager, process manager, scheduler, placement rule
    nodes/        node implementation for core, manager, site and client roles
    metrics/      JSON-lines event logger
    config/       cluster.json (nodes, capacities, seed, link settings, policies)
    experiments/  week2_load.py
    docs/         templates for the weekly log and experiments

## Metric definitions (Week 2)
- Throughput = completed jobs / (time of last completion - time of first submission)
- Latency = T_response - T_request per job (client clock)
- Jitter = standard deviation of latency
- Packet loss = dropped messages / messages sent through the emulated link
- CPU / memory = psutil samples of each worker node process

## Known limitations (state these in your report)
- Job "CPU work" is simulated with timed sleeps, so measured process CPU% is low. Report the
  simulated resource accounting (ResourceManager) alongside it, or add real CPU work later.
- Each node models a single CPU (one job slice at a time).
- All nodes run on one machine, so network conditions come from the emulated link, not a real network.
- Nodes know each other's addresses from config.json (static naming). Real naming comes in Week 10.
