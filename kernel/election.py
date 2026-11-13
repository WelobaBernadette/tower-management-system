"""Bully leader election among the regional manager nodes (Milestone 4).

Every manager has a rank (mgr1 lowest ... mgrN highest). The highest-ranked LIVE
manager must end up as leader, and every live manager must agree on who that is.

Messages (all between managers):
  ELECTION     lower -> higher : "I think the leader is gone; are you alive?"  (reply: OK)
  OK           higher -> lower : "I'm alive and higher than you; I'll take over."
  COORDINATOR  winner -> lower : "I am the leader."
  LEADER_HB    leader -> all   : periodic "still alive" signal (failure detector)

Rules:
  1. A manager that suspects the leader (no LEADER_HB for `leader_timeout` s) or has
     no leader at start-up sends ELECTION to every HIGHER manager.
  2. If any higher manager answers OK, it gives up and waits for a COORDINATOR.
  3. If none answers (dead or unreachable), it declares itself leader and sends
     COORDINATOR to every LOWER manager.
  4. A manager that receives ELECTION replies OK and starts its own election.
"""
import asyncio
import time


def _num(name):
    return int("".join(ch for ch in name if ch.isdigit()) or 0)


class BullyElection:
    def __init__(self, node):
        self.node = node
        cfg = node.cfg
        self.peers = sorted((n["name"] for n in cfg["nodes"] if n["role"] == "manager"), key=_num)
        self.rank = {p: i for i, p in enumerate(self.peers)}
        self.me = node.name
        self.leader = None
        self.electing = False
        self.last_hb = time.time()
        self.election_msgs = 0     # ELECTION + OK + COORDINATOR messages sent by this node
        self.hb_msgs = 0           # LEADER_HB messages sent by this node
        self.hb_interval = cfg.get("leader_hb_interval", 0.5)
        self.leader_timeout = cfg.get("leader_timeout", 1.5)
        self.election_timeout = cfg.get("election_timeout", 1.0)
        self.startup_grace = cfg.get("election_startup_grace", 2.0)
        self.coordinator_wait = cfg.get("coordinator_wait", 2.0)

    # ---------- background loops ----------
    def start(self):
        asyncio.create_task(self._monitor())
        asyncio.create_task(self._heartbeat())

    async def _heartbeat(self):
        """If I am the leader, tell everyone I'm alive."""
        while True:
            await asyncio.sleep(self.hb_interval)
            if self.leader == self.me:
                for p in self.peers:
                    if p != self.me:
                        self.hb_msgs += 1
                        asyncio.create_task(self.node.send(p, self.node.msg(p, "LEADER_HB")))

    async def _monitor(self):
        """Failure detector: no leader, or leader silent for too long => start an election."""
        born = time.time()
        while True:
            await asyncio.sleep(0.25)
            if self.electing:
                continue
            if self.leader is None:
                if time.time() - born > self.startup_grace:
                    asyncio.create_task(self.start_election("no_leader"))
            elif self.leader != self.me and time.time() - self.last_hb > self.leader_timeout:
                self.node.log.event("leader_suspected", leader=self.leader, lc=self.node.lc)
                asyncio.create_task(self.start_election("leader_timeout"))

    # ---------- the algorithm ----------
    async def start_election(self, reason):
        if self.electing:
            return
        self.electing = True
        self.leader = None
        self.node.lc += 1
        self.node.log.event("election_started", reason=reason, lc=self.node.lc)
        higher = [p for p in self.peers if self.rank[p] > self.rank[self.me]]

        async def ask(p):
            self.election_msgs += 1
            r = await self.node.send(p, self.node.msg(p, "ELECTION"), wait_reply=True,
                                     timeout=self.election_timeout)
            return r is not None and r.type == "OK"

        got_ok = any(await asyncio.gather(*[ask(p) for p in higher])) if higher else False
        if not got_ok:
            self.become_leader()
            return
        # A higher manager is alive and will run its own election: wait for its COORDINATOR.
        deadline = time.time() + self.coordinator_wait
        while time.time() < deadline and self.leader is None:
            await asyncio.sleep(0.05)
        if self.leader is None:            # it never announced itself: try again
            self.electing = False
            await self.start_election("coordinator_timeout")

    def become_leader(self):
        self.leader = self.me
        self.electing = False
        self.node.lc += 1
        self.node.log.event("leader_elected", leader=self.me, role="self", lc=self.node.lc)
        for p in self.peers:
            if self.rank[p] < self.rank[self.me]:
                self.election_msgs += 1
                asyncio.create_task(self.node.send(p, self.node.msg(p, "COORDINATOR", {"leader": self.me})))

    # ---------- message handlers (called from Node.handle) ----------
    def on_election(self, m):
        self.election_msgs += 1            # the OK reply
        asyncio.create_task(self.start_election("challenged"))
        return self.node.msg(m.sender, "OK")

    def on_coordinator(self, m):
        self.leader = m.payload["leader"]
        self.electing = False
        self.last_hb = time.time()
        self.node.log.event("leader_elected", leader=self.leader, role="follower", lc=self.node.lc)

    def on_leader_hb(self, m):
        self.last_hb = time.time()

    def stats(self):
        return {"leader": self.leader, "electing": self.electing,
                "election_msgs": self.election_msgs, "hb_msgs": self.hb_msgs}
