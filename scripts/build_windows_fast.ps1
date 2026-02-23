param(
  [switch]$WithInstaller,
  [switch]$WithDeps,
  [switch]$Clean,
  [switch]$SkipUiAssets
)

$ErrorActionPreference = "Stop"
$buildScript = Join-Path $PSScriptRoot "build_windows.ps1"

$buildParams = @{}
if (-not $WithInstaller) {
  $buildParams["NoInstaller"] = $true
}
if (-not $WithDeps) {
  $buildParams["SkipDeps"] = $true
  $buildParams["SkipPyInstallerUpgrade"] = $true
}
if (-not $Clean) {
  $buildParams["NoClean"] = $true
}
if ($SkipUiAssets) {
  $buildParams["SkipUiAssets"] = $true
}

$echo = ($buildParams.GetEnumerator() | ForEach-Object { "-$($_.Key)" }) -join " "
Write-Host "Fast build options: $echo"
& $buildScript @buildParams
