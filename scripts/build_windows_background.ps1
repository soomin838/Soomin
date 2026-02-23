param(
  [switch]$NoInstaller,
  [switch]$SkipDeps,
  [switch]$SkipPyInstallerUpgrade,
  [switch]$NoClean,
  [switch]$SkipUiAssets
)

$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\.."
$logDir = Join-Path $root "storage\logs\build"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logPath = Join-Path $logDir ("build_" + (Get-Date -Format "yyyyMMdd_HHmmss") + ".log")

$buildScript = Join-Path $PSScriptRoot "build_windows.ps1"
$argList = @(
  "-ExecutionPolicy", "Bypass",
  "-File", "`"$buildScript`""
)
if ($NoInstaller) {
  $argList += "-NoInstaller"
}
if ($SkipDeps) {
  $argList += "-SkipDeps"
}
if ($SkipPyInstallerUpgrade) {
  $argList += "-SkipPyInstallerUpgrade"
}
if ($NoClean) {
  $argList += "-NoClean"
}
if ($SkipUiAssets) {
  $argList += "-SkipUiAssets"
}

$argLine = ($argList -join " ") + " *> `"$logPath`""
$proc = Start-Process -FilePath "powershell" -ArgumentList $argLine -WindowStyle Minimized -PassThru

Write-Host "Background build started."
Write-Host "PID: $($proc.Id)"
Write-Host "Log: $logPath"
