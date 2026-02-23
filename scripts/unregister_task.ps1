$ErrorActionPreference = "Stop"
$taskName = "RezeroAgent"
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
  Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
  Write-Host "Scheduled task '$taskName' removed."
} else {
  Write-Host "Task '$taskName' does not exist."
}
