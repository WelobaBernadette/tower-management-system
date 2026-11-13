# Run guide: Milestones 5 and 6 (Debian VM)

## 0. One-time setup (about 3 minutes)
Your former files are all in the new zip (nothing removed); Milestones 5 and 6 are additions.
1. PowerShell window: copy the zip to the VM (same port as your ssh command):
       C:\Windows\System32\OpenSSH\scp.exe -P 2222 .\tower_fm_snapshot3.zip victor@127.0.0.1:~/
2. SSH window (`C:\Windows\System32\OpenSSH\ssh.exe -p 2222 victor@127.0.0.1`): back up, then unzip over your folder.
   Your saved results/ folder is skipped so your existing results stay untouched.
       cd ~
       cp -r tower_fm tower_fm_backup
       unzip -o tower_fm_snapshot3.zip -x "tower_fm/results/*"
       cd tower_fm
       python3 cluster.py
   (If `unzip` is missing: `sudo apt install -y unzip`.) The last line of cluster.py should now also contain txn_msgs and cc_strategy.
   Undo everything: `rm -rf ~/tower_fm && mv ~/tower_fm_backup ~/tower_fm`.
3. If your VM's nodes/node.py had edits that are not in the snapshot, compare first:
       diff ~/tower_fm_backup/nodes/node.py ~/tower_fm/nodes/node.py

## 1. Milestone 5: 2PC and 3PC
**Demo (about 1 minute, use this on Friday):**
    python3 -m experiments.week5_transactions --demo
What you will see:
- Part A: 2PC and 3PC dispatches. Expect about 12 messages per dispatch for 2PC and 18 for 3PC.
- Part B: the coordinator (mgr2) is killed after collecting votes. With 2PC the other nodes stay in doubt
  until it restarts (about 5 s); with 3PC the participants resolve it themselves in about 1.4 s without it.
  Then a participant (cloud) is killed after voting YES, restarts, and reaches the same outcome.
Say out loud: **Failure -> Detection -> Recovery -> Service Restoration**
(`txn_fault_crash` in logs/week5/mgr2.jsonl -> `txn_terminate` / `txn_blocked` on the participants -> `txn_state` COMMITTED or ABORTED on every node).

**Full run for the report (about 8-15 minutes on the 1-CPU VM; use nohup + tail -f like your other long runs):**
    nohup python3 -m experiments.week5_transactions > week5_output.txt 2>&1 &
    tail -f week5_output.txt
Creates `results/week5_failure_free.csv` (Table 5.1) and `results/week5_faults.csv` (Table 5.2).
`--quick` runs fewer trials if you are short of time.

## 2. Milestone 6: locks, race, deadlock
**Demo (about 1 minute):**
    python3 -m experiments.week6_deadlock --demo
Shows: (1) 3 sites adding to one shared counter, unsafe loses updates, locked loses none;
(2) the classic two-task deadlock under `none` (stuck, with the resource-allocation graph printed and saved to
`results/week6_rag_deadlock.dot`), `detect_priority` (cycle found, routine task aborted, both finish) and `prevent`.

**Full run for the report (about 20-40 minutes on the 1-CPU VM):**
    nohup python3 -m experiments.week6_deadlock > week6_output.txt 2>&1 &
    tail -f week6_output.txt
Creates `results/week6_race.csv` (Table 6.1), `results/week6_deadlock.csv` (Table 6.2) and
`results/week6_detect_interval.csv` (Table 6.3). `--quick` = 1 trial if time is very short.

Draw the graph (Figure 6.1):
    sudo apt install -y graphviz
    dot -Tpng results/week6_rag_deadlock.dot -o results/rag.png

## 3. Order for tonight (report due 23:59)
1. Setup and sanity check (10 min)
2. `week5_transactions` full run (8 min) -> fill Tables 5.1 and 5.2
3. `week6_deadlock` full run (20 min, start it, then write the Milestone 5 analysis while it runs)
4. `python3 -m experiments.week6_deadlock --demo` once to create the .dot file, render the PNG
5. Fill the report placeholders, export PDF, push to GitHub
Run the two full experiments one after the other, not at the same time: both measure real time, so running
together distorts the numbers.

## 4. Push to GitHub
    git add -A && git commit -m "Milestones 5 and 6: 2PC/3PC, locks, deadlock management" && git push
(`logs/` and temporary configs are ignored by `.gitignore`; `results/` is included.)

## 5. If something goes wrong
| Symptom | Fix |
|---|---|
| `Address already in use` or odd leftovers | `pkill -f nodes.run` then run again |
| `RuntimeError: mgr2 never became leader` / `no leader elected` | a previous run is still alive: `pkill -f nodes.run`, wait 5 s, retry |
| Milestone 5 results look strange after an aborted run | `rm -rf logs/week5` (a stale write-ahead log is replayed on start; the script also clears it) |
| Milestone 6 run is slow at 16 tasks | normal: detection resolves one deadlock per round; this is the result being measured |
| Numbers differ from the report draft | expected: real-time runs vary; use YOUR means and say so |
| VM runs out of memory | give the VM 2-3 GB RAM; close the browser inside the VM |

## 6. What each result means (for the oral defense)
- 12 vs 18 messages: 2PC = 4n, 3PC = 6n for n = 3 participants (the extra phase costs 2n).
- About 167 vs 250 ms: 2 vs 3 round trips of an 80 ms link.
- 2PC blocked, 3PC not: after the coordinator dies, 3PC participants can tell from their own state
  (READY vs PRECOMMITTED) what is safe; 2PC participants in READY cannot.
- 3PC caveat: only non-blocking if the network never partitions.
- Prevention vs detection: ordering removes circular wait; detection pays for each cycle with an abort and
  redone work; power-aware victims protect critical alarms.
