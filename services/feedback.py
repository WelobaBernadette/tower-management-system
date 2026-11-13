"""Feedback service: the downward path Cloud -> Core (Milestone 3, Edge <-> Core <-> Cloud).

Before this, data only flowed upward (site -> manager -> core -> cloud). Now the loop closes:

  UP    core  -> cloud  INCIDENT_RECORD   every incident the core sees is recorded as history
  DOWN  cloud -> core   POLICY_UPDATE     the cloud finds repeat-offender sites (>= watch_threshold
                                          incidents) and pushes a watch-list to the core; the core
                                          then flags new incidents at those sites as high priority
  PULL  any   -> cloud  HISTORY_QUERY     reply HISTORY_REPLY with a site's incident history

Enabled with  "cloud_feedback": true  in the cluster config (default off, so earlier
experiments are unchanged). Optional keys: watch_threshold (2), policy_interval (1.0 s).
"""
import asyncio
import time


class FeedbackService:
    def __init__(self, node):
        self.node = node
        cfg = node.cfg
        self.enabled = cfg.get("cloud_feedback", False)
        self.threshold = cfg.get("watch_threshold", 2)
        self.interval = cfg.get("policy_interval", 1.0)
        self.history = {}          # cloud: site -> list of {"kind", "cause", "core_ts"}
        self.watchlist = {}        # core: site -> incident count, as last pushed by the cloud
        self.seq = 0               # cloud: policy version number
        self._last = None          # cloud: last watch-list pushed
        self.records_sent = 0      # core -> cloud
        self.policies_sent = 0     # cloud -> core
        self.policies_received = 0
        self.boosted = 0           # core: incidents flagged high priority thanks to the watch-list
        self.queries_served = 0

    def _cloud(self):
        return next((n["name"] for n in self.node.cfg["nodes"] if n["role"] == "cloud"), None)

    # ---------- core side ----------
    def on_incident(self, payload, sender, kind):
        """Called by the core when a manager escalates (OPEN) or resolves an incident."""
        node = self.node
        if not self.enabled or node.role != "core":
            return
        site = payload.get("site")
        if kind == "OPEN" and site in self.watchlist:
            payload["priority"] = "high"           # stored with the incident by the caller
            self.boosted += 1
            node.log.event("priority_boost", site=site, past_incidents=self.watchlist[site], lc=node.lc)
        cloud = self._cloud()
        if cloud:
            self.records_sent += 1
            rec = {"site": site, "kind": kind, "cause": payload.get("cause"), "core_ts": time.time()}
            asyncio.create_task(node.send(cloud, node.msg(cloud, "INCIDENT_RECORD", rec)))

    def on_policy(self, payload):
        """Core receives the cloud's watch-list."""
        self.watchlist = dict(payload.get("watchlist", {}))
        self.policies_received += 1
        self.node.log.event("policy_received", seq=payload.get("seq"), watchlist=sorted(self.watchlist),
                            lc=self.node.lc)

    # ---------- cloud side ----------
    def start(self):
        if self.enabled and self.node.role == "cloud":
            asyncio.create_task(self._policy_loop())

    def on_record(self, payload):
        site = payload.get("site")
        self.history.setdefault(site, []).append(
            {"kind": payload.get("kind"), "cause": payload.get("cause"), "core_ts": payload.get("core_ts")})
        up_ms = round(1000 * (time.time() - payload.get("core_ts", time.time())), 1)
        self.node.log.event("history_recorded", site=site, incident_kind=payload.get("kind"), up_ms=up_ms)

    def _offenders(self):
        counts = {s: sum(1 for r in recs if r["kind"] == "OPEN") for s, recs in self.history.items()}
        return {s: c for s, c in counts.items() if c >= self.threshold}

    async def _policy_loop(self):
        core = self.node.core_name
        while True:
            await asyncio.sleep(self.interval)
            offenders = self._offenders()
            if offenders != self._last:            # push only when the watch-list changes
                self._last = offenders
                self.seq += 1
                self.policies_sent += 1
                self.node.log.event("policy_pushed", seq=self.seq, watchlist=sorted(offenders),
                                    lc=self.node.lc)
                await self.node.send(core, self.node.msg(core, "POLICY_UPDATE",
                                                         {"seq": self.seq, "watchlist": offenders}))

    def history_for(self, site):
        self.queries_served += 1
        if site is None:
            return {"sites": {s: sum(1 for r in recs if r["kind"] == "OPEN") for s, recs in self.history.items()}}
        recs = self.history.get(site, [])
        opens = [r for r in recs if r["kind"] == "OPEN"]
        return {"site": site, "incidents": len(opens),
                "last_cause": opens[-1]["cause"] if opens else None,
                "watch_listed": site in self._offenders()}

    def stats(self):
        return {"fb_records_sent": self.records_sent, "fb_policies_sent": self.policies_sent,
                "fb_policies_received": self.policies_received, "fb_boosted": self.boosted,
                "fb_queries": self.queries_served, "fb_watchlist": sorted(self.watchlist)}
