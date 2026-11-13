"""Milestone 5: distributed transactions (2PC vs 3PC) on the real cluster.

Every dispatch changes THREE nodes atomically: core (team registry), cloud (audit log) and the site's
manager (dispatch ledger). The coordinator is the Bully-elected leader (mgr2). Numbers come from real node
processes talking over TCP; faults are real process crashes (os._exit) and restarts.

  Part A  Failure-free cost      N dispatches per protocol, one at a time.
          H5.1: 3PC costs ~1.5x the messages (6n vs 4n) and about one extra round trip of latency.
  Part B  Failures               fault injected into one dispatch, then we watch recovery:
            coord_after_votes       coordinator dies after collecting YES votes
            coord_after_decision    coordinator dies after logging its decision, before telling anyone
            coord_partial_precommit (3PC) dies after PRECOMMIT reached only the first participant
            coord_after_precommit   (3PC) dies after every participant has PRECOMMIT
            cloud_crash_before_vote a participant dies before it can vote
            cloud_crash_after_vote  a participant dies right after voting YES
            cloud_votes_no          a participant votes NO (no crash)
            conflict_same_team      two concurrent dispatches want the SAME team (isolation: exactly one wins)
          H5.2: 2PC participants stay BLOCKED (in doubt, team stays reserved) until the dead
                coordinator restarts; 3PC participants are resolved by the new leader without it.
          H5.3: in every scenario, every node ends in the same state (0 atomicity violations).
          H5.4: a participant that crashes after voting YES recovers to the common outcome.

Controlled variables: 1 site + 2 managers + core + cloud + client, link 80 ms +-2 ms one way, no loss,
tx_timeout 1.0 s, coordinator/participant restart after DOWN_S seconds, fresh WAL directory per run.

    python3 -m experiments.week5_transactions --demo      narrated (about 1 minute)
    python3 -m experiments.week5_transactions             full run (add --quick for fewer trials)
Writes results/week5_failure_free.csv and results/week5_faults.csv
"""
import asyncio
import csv
import json
import os
import shutil
import statistics
import subprocess
import sys
import time

from cluster import launch, shutdown, wait_registered
from kernel.topology import build
from nodes.node import Node, load_config

BASE, TMP, LOGS = "config/cluster.json", "config/_week5_tmp.json", "logs/week5"
PARTS = ("core", "cloud", "mgr1")          # participants for site1 (its manager is mgr1) when mgr2 coordinates
DOWN_S = 4.0                               # how long a crashed node stays down
TERMINAL = ("COMMITTED", "ABORTED")


async def ask(client, name, typ, payload=None, timeout=3.0):
    return await client.send(name, client.msg(name, typ, payload or {}), wait_reply=True,
                             emulate=False, timeout=timeout)


class Cluster:
    def __init__(self, base_port=27000):
        self.cfg = build(load_config(BASE), 1, 2, base_port=base_port)
        self.cfg["log_dir"] = LOGS
        # 80 ms one-way link (WAN-like). Also makes the injected "participant dies 50 ms after voting"
        # land BEFORE the COMMIT message arrives, so the participant really is in doubt when it dies.
        self.cfg["link"] = {"delay_ms": 80, "jitter_ms": 2, "loss": 0.0}
        self.names = [n["name"] for n in self.cfg["nodes"] if n["role"] != "client"]
        self.n_tx = 0

    async def __aenter__(self):
        shutil.rmtree(LOGS, ignore_errors=True)            # a stale WAL would be replayed at start-up
        with open(TMP, "w") as f:
            json.dump(self.cfg, f)
        self.procs = launch(TMP, self.names)
        self.client = Node(self.cfg, "client")
        await self.client.start()
        await wait_registered(self.client, [n for n in self.names if n != "core"], timeout=40)
        await self.wait_leader()
        return self

    async def __aexit__(self, *exc):
        await shutdown(self.client, self.names)
        self.client.tp.server.close()
        await self.client.tp.server.wait_closed()
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    async def wait_leader(self, timeout=40):
        t0 = time.time()
        while time.time() - t0 < timeout:
            sts = [await ask(self.client, m, "STATS", timeout=1.0) for m in ("mgr1", "mgr2")]
            if all(sts) and all(s.payload["leader"] == "mgr2" for s in sts):
                return
            await asyncio.sleep(0.3)
        raise RuntimeError("mgr2 never became leader")

    def restart(self, name):
        i = self.names.index(name)
        self.procs[i] = subprocess.Popen([sys.executable, "-m", "nodes.run", "--config", TMP, "--name", name])

    def next_ids(self):
        self.n_tx += 1
        k = self.n_tx
        return f"tx{k}", f"T{(k - 1) % 40 + 1}", f"inc{k}"

    async def begin(self, mode, faults=None, timeout=12.0):
        """Send one dispatch to the leader. Returns (tx_id, reply payload or None, client RTT ms)."""
        tx, team, inc = self.next_ids()
        t0 = time.time()
        r = await ask(self.client, "mgr2", "DISPATCH", {
            "txid": tx, "proto": mode, "team": team, "incident": inc, "site": "site1",
            "faults": faults or {}}, timeout=timeout)
        return tx, (r.payload if r else None), round((time.time() - t0) * 1000, 1)

    async def dump(self, name):
        r = await ask(self.client, name, "TXN_DUMP", timeout=1.0)
        return r.payload if r else None

    async def term_msgs(self):
        """Termination/re-send messages sent by the participants and the coordinator so far."""
        total = 0
        for n in PARTS + ("mgr2",):
            r = await ask(self.client, n, "STATS", timeout=1.0)
            total += r.payload.get("txn_term_msgs", 0) if r else 0
        return total

    async def states(self, tx):
        out = {}
        for n in PARTS:
            r = await ask(self.client, n, "TXN_STATE_REQ", {"txid": tx}, timeout=1.0)
            out[n] = (r.payload["state"] or "UNKNOWN") if r else "DOWN"
        return out



# --------------------------------------------------------------------------- Part A
async def failure_free(cl, n, say):
    rows = []
    for mode in ("2pc", "3pc"):
        lat, rtt, msgs = [], [], []
        t_start = time.time()
        for _ in range(n):
            tx, res, ms = await cl.begin(mode)
            if res and res.get("outcome") == "COMMITTED":
                lat.append(res["latency_ms"])
                rtt.append(ms)
                msgs.append(res["msgs"])
        span = time.time() - t_start
        p95 = sorted(lat)[int(0.95 * (len(lat) - 1))] if lat else 0
        row = {"protocol": mode.upper(), "committed": len(lat), "attempted": n,
               "messages_per_tx": round(statistics.mean(msgs), 2) if msgs else 0,
               "model_messages": 12 if mode == "2pc" else 18,
               "latency_mean_ms": round(statistics.mean(lat), 1) if lat else 0,
               "latency_p95_ms": p95, "latency_stdev_ms": round(statistics.pstdev(lat), 1) if lat else 0,
               "throughput_tx_s": round(len(lat) / span, 2)}
        say(f"   {mode.upper()}: {row['committed']}/{n} committed, {row['messages_per_tx']} msgs/tx "
            f"(model {row['model_messages']}), latency {row['latency_mean_ms']} ms mean, {p95} ms p95")
        rows.append(row)
    return rows


# --------------------------------------------------------------------------- Part B
SCENARIOS = [  # name, mode, faults, expected outcome
    ("coord_after_votes", "2pc", {"crash_at": "after_votes"}, "ABORT"),
    ("coord_after_votes", "3pc", {"crash_at": "after_votes"}, "ABORT"),
    ("coord_after_decision", "2pc", {"crash_at": "after_decision"}, "COMMIT"),
    ("coord_after_decision", "3pc", {"crash_at": "after_decision"}, "COMMIT"),
    ("coord_partial_precommit", "3pc", {"crash_at": "partial_precommit"}, "COMMIT"),
    ("coord_after_precommit", "3pc", {"crash_at": "after_precommit"}, "COMMIT"),
    ("cloud_crash_before_vote", "2pc", {"crash_before_vote": "cloud"}, "ABORT"),
    ("cloud_crash_before_vote", "3pc", {"crash_before_vote": "cloud"}, "ABORT"),
    ("cloud_crash_after_vote", "2pc", {"crash_after_vote": "cloud"}, "COMMIT"),
    ("cloud_crash_after_vote", "3pc", {"crash_after_vote": "cloud"}, "COMMIT"),
    ("cloud_votes_no", "2pc", {"vote_no": "cloud"}, "ABORT"),
    ("cloud_votes_no", "3pc", {"vote_no": "cloud"}, "ABORT"),
]


async def fault_run(cl, name, mode, faults, expected, say):
    crashed = any(k in faults for k in ("crash_at", "crash_after_vote", "crash_before_vote"))
    victim = "mgr2" if "crash_at" in faults else "cloud"
    tx, res, rtt = await cl.begin(mode, faults)
    t_crash = time.time()
    say(f"   [{mode.upper()} {name}] reply to client: {res['outcome'] if res else 'none (node died)'}")
    term0 = await cl.term_msgs()
    restarted_at, blocked_seen, resolved_before_restart = None, False, False
    final, t_done = {}, None
    while time.time() - t_crash < 45:
        if crashed and restarted_at is None and time.time() - t_crash >= DOWN_S:
            cl.restart(victim)
            restarted_at = time.time()
            say(f"      restarting {victim} after {DOWN_S:.0f} s down")
        st = await cl.states(tx)
        alive = {k: v for k, v in st.items() if v != "DOWN"}
        if crashed and restarted_at is None and any(v in ("PREPARED", "PRECOMMITTED") for v in alive.values()):
            blocked_seen = True                       # someone is in doubt while the node is still dead
        if all(v in TERMINAL for v in st.values()):
            final, t_done = st, time.time()
            resolved_before_restart = crashed and restarted_at is None
            break
        await asyncio.sleep(0.15)
    if crashed and restarted_at is None:              # resolved (or timed out) before the planned restart
        cl.restart(victim)
    term = await cl.term_msgs() - term0
    outcomes = set(final.values())
    consistent = len(outcomes) == 1 and bool(final)
    outcome = ("COMMIT" if outcomes == {"COMMITTED"} else "ABORT" if outcomes == {"ABORTED"} else "MIXED/UNRESOLVED")
    viol = 0
    for n in PARTS:
        d = await cl.dump(n)
        viol += d["violations"] if d else 0
    core = await cl.dump("core")
    row = {"scenario": name, "protocol": mode.upper(), "outcome": outcome, "expected": expected,
           "consistent": consistent, "in_doubt_while_node_down": blocked_seen,
           "resolved_without_restart": resolved_before_restart,
           "time_to_all_terminal_s": round(t_done - t_crash, 2) if t_done else "",
           "reserved_teams_left": len(core["reserved"]) if core else "", "violations": viol,
           "termination_msgs": term}
    say(f"      -> {outcome} on all nodes: {consistent}; in doubt while node down: {blocked_seen}; "
        f"resolved without restart: {resolved_before_restart}; time {row['time_to_all_terminal_s']} s")
    if crashed:                                       # let the cluster settle before the next scenario
        await asyncio.sleep(1.0)
        try:
            await cl.wait_leader(timeout=40)
        except RuntimeError:
            pass
    return row


async def conflict_run(cl, mode, say):
    """Two concurrent dispatches for the SAME team (different incidents): isolation says exactly one wins."""
    tx1, team, inc1 = cl.next_ids()
    tx2, _, inc2 = cl.next_ids()
    reqs = [ask(cl.client, "mgr2", "DISPATCH", {"txid": t, "proto": mode, "team": team, "incident": i,
                                                 "site": "site1", "faults": {}}, timeout=12.0)
            for t, i in ((tx1, inc1), (tx2, inc2))]
    res = [r.payload["outcome"] if r else "NONE" for r in await asyncio.gather(*reqs)]
    await asyncio.sleep(0.5)
    viol = 0
    for n in PARTS:
        viol += (await cl.dump(n))["violations"]
    ok = sorted(res) == ["ABORTED", "COMMITTED"]
    say(f"   [{mode.upper()} conflict_same_team] outcomes {res} -> exactly one winner: {ok}")
    return {"scenario": "conflict_same_team", "protocol": mode.upper(), "outcome": "+".join(sorted(res)),
            "expected": "ABORTED+COMMITTED", "consistent": ok, "in_doubt_while_node_down": False,
            "resolved_without_restart": False, "time_to_all_terminal_s": "", "reserved_teams_left": "",
            "violations": viol, "termination_msgs": 0}


# ------------------------------------------------------------------------------ main
def write_csv(path, rows):
    os.makedirs("results", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print("saved", path)


async def main():
    demo, quick = "--demo" in sys.argv, "--quick" in sys.argv
    say = print
    async with Cluster() as cl:
        say("PART A: failure-free cost of 2PC vs 3PC (3 participants)")
        rows = await failure_free(cl, 10 if demo else (15 if quick else 40), say)
        if not demo:
            write_csv("results/week5_failure_free.csv", rows)
        say("\nPART B: failures (coordinator = mgr2, participants = core, cloud, mgr1)")
        scen = [s for s in SCENARIOS if not demo or (s[0] in ("coord_after_votes", "cloud_crash_after_vote"))]
        trials = 1 if (demo or quick) else 3
        out = []
        for name, mode, faults, expected in scen:
            for _ in range(trials):
                out.append(await fault_run(cl, name, mode, faults, expected, say))
        if not demo:
            for mode in ("2pc", "3pc"):
                for _ in range(trials):
                    out.append(await conflict_run(cl, mode, say))
        if not demo:
            write_csv("results/week5_faults.csv", out)
        bad = [r for r in out if not r["consistent"] or r["violations"]]
        say(f"\nAtomicity violations or inconsistent runs: {len(bad)} of {len(out)}")


if __name__ == "__main__":
    asyncio.run(main())
