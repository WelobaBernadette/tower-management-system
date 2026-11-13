"""Milestone 4, part 2: Bully leader election among manager nodes.

For each cluster size (number of managers) we:
  1. start the cluster and wait until every manager agrees the highest-ranked one leads,
  2. measure the steady-state heartbeat overhead,
  3. CRASH the leader process, and measure
       detection delay      = crash -> first manager starts an election
       election duration    = first election start -> last manager learns the new leader
       synchronization delay= crash -> all live managers agree (detection + election)
       election messages    = ELECTION + OK + COORDINATOR sent, versus theory
       consistency          = do all live managers name the same, correct leader?

    python3 -m experiments.week4_election          full sweep (2,4,6,8 managers x 3 trials)
    python3 -m experiments.week4_election --demo   one narrated run with 4 managers
Writes results/week4_election.csv
"""
import asyncio
import csv
import json
import os
import statistics
import sys
import time

from cluster import launch, shutdown, wait_registered
from kernel.topology import build
from nodes.node import Node, load_config

BASE = "config/cluster.json"
TMP = "config/_elect_tmp.json"
SIZES = (2, 4, 6, 8)
TRIALS = 3


def read(path):
    rows = []
    try:
        with open(path) as f:
            for line in f:
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    pass
    except OSError:
        pass
    return rows


async def stats(client, name):
    r = await client.send(name, client.msg(name, "STATS"), wait_reply=True, emulate=False, timeout=1.5)
    return r.payload if r else None


async def wait_agree(client, mgrs, expect, timeout=25):
    """Poll until every manager in `mgrs` reports `expect` as leader. Returns their stats or None."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        sts = [await stats(client, m) for m in mgrs]
        if all(sts) and all(s["leader"] == expect for s in sts):
            return sts
        await asyncio.sleep(0.2)
    return None


def model_msgs(n):
    """Simple model: leader (rank n) crashes and each of the n-1 survivors runs exactly ONE election.
    ELECTION: n(n-1)/2   OK: (n-1)(n-2)/2   COORDINATOR: n-2.
    Real runs can exceed it: a manager that already won re-runs its election when a slower
    lower manager's ELECTION arrives late."""
    return n * (n - 1) // 2 + (n - 1) * (n - 2) // 2 + (n - 2)


async def run_once(base_cfg, n_mgr, trial, verbose=False):
    say = print if verbose else (lambda *a, **k: None)
    cfg = build(base_cfg, 2, n_mgr, base_port=24000 + n_mgr * 40 + trial)
    cfg["log_dir"] = f"logs/elect_{n_mgr}m_t{trial}"
    with open(TMP, "w") as f:
        json.dump(cfg, f)
    names = [n["name"] for n in cfg["nodes"] if n["role"] != "client"]
    mgrs = [f"mgr{i + 1}" for i in range(n_mgr)]
    procs = launch(TMP, names)
    client = Node(cfg, "client")
    await client.start()
    try:
        await wait_registered(client, [n for n in names if n != "core"], timeout=40)
        say(f"1) {n_mgr} managers started. Waiting for them to elect the highest ({mgrs[-1]}) ...")
        first = await wait_agree(client, mgrs, mgrs[-1], timeout=30)
        if first is None:
            return {"managers": n_mgr, "trial": trial, "error": "no initial agreement"}
        say(f"   all managers agree: leader = {mgrs[-1]}")

        t_a, hb_a = time.time(), sum(s["hb_msgs"] for s in first)
        await asyncio.sleep(3.0)
        mid = [await stats(client, m) for m in mgrs]
        hb_rate = (sum(s["hb_msgs"] for s in mid) - hb_a) / (time.time() - t_a)
        say(f"2) steady state: leader heartbeats cost {hb_rate:.1f} messages/s")

        victim, survivors, expect = mgrs[-1], mgrs[:-1], mgrs[-2]
        pre = sum(s["election_msgs"] for s in mid if s["node"] != victim)
        say(f"3) CRASHING the leader ({victim}) ...")
        t_kill = time.time()
        procs[names.index(victim)].terminate()
        agree = await wait_agree(client, survivors, expect, timeout=30)
        consistent = agree is not None
        if not consistent:
            return {"managers": n_mgr, "trial": trial, "error": "no agreement after crash", "consistent": False}
        msgs = sum(s["election_msgs"] for s in agree) - pre

        starts, elected = [], []
        for m in survivors:
            for e in read(f"{cfg['log_dir']}/{m}.jsonl"):
                if e["ts"] > t_kill and e["event"] == "election_started":
                    starts.append(e["ts"])
                if e["ts"] > t_kill and e["event"] == "leader_elected" and e["leader"] == expect:
                    elected.append(e["ts"])
        first_start, last_elected = min(starts), max(elected)
        row = {"managers": n_mgr, "trial": trial, "consistent": True, "new_leader": expect,
               "detection_s": round(first_start - t_kill, 3),
               "election_s": round(last_elected - first_start, 3),
               "sync_delay_s": round(last_elected - t_kill, 3),
               "election_msgs": msgs, "best_case_msgs": n_mgr - 1, "model_msgs": model_msgs(n_mgr),
               "elections_started": len(starts), "hb_msgs_per_s": round(hb_rate, 1)}
        say(f"4) new leader = {expect}; all {len(survivors)} live managers agree")
        say(f"   detection {row['detection_s']}s + election {row['election_s']}s = {row['sync_delay_s']}s total")
        say(f"   election messages: {msgs}  (best case {row['best_case_msgs']}, one-election-each model {row['model_msgs']})")
        return row
    finally:
        await shutdown(client, names)
        client.tp.server.close()
        await client.tp.server.wait_closed()
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()


async def main():
    base_cfg = load_config(BASE)
    out = "results/week4_election.csv"
    if "--demo" in sys.argv:
        await run_once(base_cfg, 4, 0, verbose=True)
        return
    os.makedirs("results", exist_ok=True)
    rows = []
    for n in SIZES:
        for t in range(TRIALS):
            r = await run_once(base_cfg, n, t)
            rows.append(r)
            print(n, "managers, trial", t, "->",
                  {k: r.get(k) for k in ("detection_s", "election_s", "sync_delay_s", "election_msgs", "consistent")})
    good = [r for r in rows if r.get("consistent")]
    fields = ["managers", "trial", "consistent", "new_leader", "detection_s", "election_s", "sync_delay_s",
              "election_msgs", "best_case_msgs", "model_msgs", "elections_started", "hb_msgs_per_s"]
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(good)
    print("\nSummary (mean over trials):")
    print(f"{'mgrs':>5} {'detect s':>9} {'elect s':>8} {'sync s':>7} {'msgs':>6} {'best':>5} {'model':>6} {'hb/s':>6} consistent")
    for n in SIZES:
        g = [r for r in good if r["managers"] == n]
        if g:
            mean = lambda k: statistics.mean(r[k] for r in g)
            print(f"{n:>5} {mean('detection_s'):>9.2f} {mean('election_s'):>8.2f} {mean('sync_delay_s'):>7.2f} "
                  f"{mean('election_msgs'):>6.1f} {n - 1:>5} {model_msgs(n):>6} {mean('hb_msgs_per_s'):>6.1f} "
                  f"{len(g)}/{TRIALS}")
    print("saved", out)


if __name__ == "__main__":
    asyncio.run(main())
