<#
  run_m56.ps1  -  run the Milestone 5 and 6 experiments in the VM over SSH and show the results in THIS window.

  Examples (from the folder that holds this file):
    powershell -ExecutionPolicy Bypass -File .\run_m56.ps1                         # demos only, about 3 min
    powershell -ExecutionPolicy Bypass -File .\run_m56.ps1 -Mode quick             # 1 trial each, about 15 min
    powershell -ExecutionPolicy Bypass -File .\run_m56.ps1 -Mode full              # for the report, about 30 min
    powershell -ExecutionPolicy Bypass -File .\run_m56.ps1 -Target victor@127.0.0.1 -Port 2222   # these are already the defaults

  What it does: kills stale node processes, runs the experiments in the VM, prints every result table here,
  saves a log next to this script (m56_run_<time>.log) and copies the VM's results folder to .\m56_results
  (UNTESTED here: I could not run PowerShell in my sandbox, so read the first run's output carefully).
#>
param(
  [string]$Target = "victor@127.0.0.1",      # user@address of the VM (same as your ssh command)
  [int]$Port = 2222,
  [string]$Dir = "~/tower_fm",               # the project folder inside the VM
  [string]$Venv = "",                        # optional: path to a venv activate script, leave empty if you do not use one
  [ValidateSet("demo","quick","full")][string]$Mode = "demo"
)

$ssh = "C:\Windows\System32\OpenSSH\ssh.exe"
$scp = "C:\Windows\System32\OpenSSH\scp.exe"
$log = "m56_run_$(Get-Date -Format 'yyyyMMdd_HHmm').log"

function Run-Remote([string]$cmd) {
  Write-Host ""
  Write-Host ">>> $cmd" -ForegroundColor Cyan
  $pre = ""
  if ($Venv -ne "") { $pre = "source $Venv; " }
  & $ssh -p $Port $Target "${pre}cd $Dir; $cmd" 2>&1 | Tee-Object -FilePath $log -Append
}

function Show-Csv([string]$file) {
  Write-Host ""
  Write-Host "=== $file ===" -ForegroundColor Yellow
  & $ssh -p $Port $Target "cd $Dir; (column -s, -t < $file 2>/dev/null || cat $file)" 2>&1 | Tee-Object -FilePath $log -Append
}

Write-Host "Mode: $Mode   Target: $Target   Folder in VM: $Dir" -ForegroundColor Green
Run-Remote "pkill -f 'nodes[.]run'; sleep 1; python3 cluster.py | tail -3"

if ($Mode -eq "demo") {
  Run-Remote "python3 -u -m experiments.week5_transactions --demo"
  Run-Remote "python3 -u -m experiments.week6_deadlock --demo"
  Run-Remote "cat results/week6_rag_deadlock.dot"
}
else {
  $flag = ""
  if ($Mode -eq "quick") { $flag = "--quick" }
  Run-Remote "python3 -u -m experiments.week5_transactions $flag | tee results/week5_run.txt"
  Run-Remote "python3 -u -m experiments.week6_deadlock $flag | tee results/week6_run.txt"
  Show-Csv "results/week5_failure_free.csv"
  Show-Csv "results/week5_faults.csv"
  Show-Csv "results/week6_race.csv"
  Show-Csv "results/week6_deadlock.csv"
  Show-Csv "results/week6_detect_interval.csv"
}

Write-Host ""
Write-Host "Copying results from the VM to .\m56_results ..." -ForegroundColor Green
& $scp -P $Port -r "${Target}:$Dir/results" ".\m56_results"
Write-Host "Log saved to $log" -ForegroundColor Green
