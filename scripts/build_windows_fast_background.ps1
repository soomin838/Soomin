param(
  [switch]$WithInstaller,
  [switch]$WithDeps,
  [switch]$Clean,
  [switch]$SkipUiAssets
)

$ErrorActionPreference = "Stop"
$buildScript = Join-Path $PSScriptRoot "build_windows_background.ps1"

$buildArgs = @()
if (-not $WithInstaller) {
  $buildArgs += "-NoInstaller"
}
if (-not $WithDeps) {
  $buildArgs += "-SkipDeps"
  $buildArgs += "-SkipPyInstallerUpgrade"
}
if (-not $Clean) {
  $buildArgs += "-NoClean"
}
if ($SkipUiAssets) {
  $buildArgs += "-SkipUiAssets"
}

Write-Host "Fast background build options: $($buildArgs -join ' ')"
& $buildScript @buildArgs
