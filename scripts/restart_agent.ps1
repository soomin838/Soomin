$ErrorActionPreference = "SilentlyContinue"

# Stop running process instances.
Get-Process RezeroAgent -ErrorAction SilentlyContinue | Stop-Process -Force

# Candidate paths (installed app first, then local dist).
$candidates = @(
  "$env:LOCALAPPDATA\\Programs\\RezeroAgent\\RezeroAgent.exe",
  "$PSScriptRoot\\..\\dist\\RezeroAgent.exe"
)

$target = $null
foreach ($p in $candidates) {
  if (Test-Path $p) {
    $target = $p
    break
  }
}

if (-not $target) {
  Write-Host "RezeroAgent executable not found."
  exit 1
}

Start-Process -FilePath $target | Out-Null
Write-Host "RezeroAgent restarted: $target"

