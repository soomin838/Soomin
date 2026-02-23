$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$exe = Join-Path $root "dist\RezeroAgent.exe"

if (-not (Test-Path $exe)) {
  throw "dist\RezeroAgent.exe not found. Build first with scripts\build_windows.ps1"
}

$taskName = "RezeroAgent"
$action = New-ScheduledTaskAction -Execute $exe
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -MultipleInstances IgnoreNew

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Force
Write-Host "Scheduled task '$taskName' registered (run at logon, single daemon instance)."
