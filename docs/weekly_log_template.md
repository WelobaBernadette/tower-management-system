# Failure-Driven Engineering Log: Week N

| Question | Response |
|---|---|
| What changed? | |
| What failed? | |
| Why did it fail? | |
| How was it fixed? | |
| What alternative was considered? | |
| What was learned? | |

## Week 1 candidate entries (things that genuinely happened while building the starter)
- Stale load data made `least_loaded` send bursts to one node -> fixed by counting each placement immediately.
- Second experiment run crashed: client port still bound from the previous run -> close the server between runs.
- Load generator sent requests one by one, so link delay capped the offered rate -> send concurrently.
