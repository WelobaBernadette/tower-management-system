"""Where should a new job run? Replaceable module: Week 3+ can swap in smarter rules."""


def choose(policy, workers, registry, core):
    if not workers:
        return None
    workers = sorted(workers)
    if policy == "least_loaded":
        target = min(workers, key=lambda w: registry[w].get("load", 0))
        registry[target]["load"] = registry[target].get("load", 0) + 1  # avoid herding on stale data
        return target
    if policy == "first":  # deliberately poor baseline: everything on one node
        return workers[0]
    core.rr = (core.rr + 1) % len(workers)
    return workers[core.rr]
