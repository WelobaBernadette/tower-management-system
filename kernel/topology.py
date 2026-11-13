"""Generate a cluster.json-shaped config for N sites split across M managers,
so Milestone 3's scalability experiment can vary cluster size programmatically."""
import copy


def build(base_cfg, n_sites, n_managers, base_port=20000):
    cfg = copy.deepcopy(base_cfg)
    nodes = [{"name": "cloud", "role": "cloud", "port": base_port, "cpu": 100, "mem": 512, "tier": "cloud"},
             {"name": "core", "role": "core", "port": base_port + 1, "cpu": 100, "mem": 512, "tier": "core"}]
    mgr_names = [f"mgr{i+1}" for i in range(n_managers)]
    for i, name in enumerate(mgr_names):
        nodes.append({"name": name, "role": "manager", "port": base_port + 10 + i, "cpu": 100, "mem": 512, "tier": "core"})
    for i in range(n_sites):
        mgr = mgr_names[i % n_managers]
        nodes.append({"name": f"site{i+1}", "role": "site", "port": base_port + 100 + i,
                     "cpu": 100, "mem": 512, "tier": "edge", "manager": mgr})
    nodes.append({"name": "client", "role": "client", "port": base_port + 999, "cpu": 100, "mem": 512, "tier": "client"})
    cfg["nodes"] = nodes
    return cfg
