"""Per-node resource manager: CPU and memory budgets with allocate/release."""


class ResourceManager:
    def __init__(self, cpu, mem):
        self.cpu_total, self.mem_total = cpu, mem
        self.cpu_used = 0
        self.mem_used = 0

    def allocate(self, cpu, mem):
        if self.cpu_used + cpu > self.cpu_total or self.mem_used + mem > self.mem_total:
            return False
        self.cpu_used += cpu
        self.mem_used += mem
        return True

    def release(self, cpu, mem):
        self.cpu_used = max(0, self.cpu_used - cpu)
        self.mem_used = max(0, self.mem_used - mem)

    def free(self):
        return {"cpu": self.cpu_total - self.cpu_used, "mem": self.mem_total - self.mem_used}
