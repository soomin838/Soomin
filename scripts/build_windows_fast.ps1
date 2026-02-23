param(
  [switch]$WithInstaller,
  [switch]$NoInstaller,
  [switch]$WithDeps,
  [switch]$Clean,
  [switch]$SkipUiAssets,
  [switch]$AutoInstaller
)

$ErrorActionPreference = "Stop"
$repoRoot = Join-Path $PSScriptRoot ".."
$buildScript = Join-Path $PSScriptRoot "build_windows.ps1"
$gitExe = "C:\Program Files\Git\cmd\git.exe"

function Test-InstallerScriptChanged {
  if (-not (Test-Path $gitExe)) { return $false }
  try {
    $changed = & $gitExe -C $repoRoot status --porcelain -- "packaging/windows/installer.iss" 2>$null
    if ($LASTEXITCODE -eq 0 -and $changed) { return $true }
  } catch {}
  try {
    $last = & $gitExe -C $repoRoot diff --name-only HEAD~1 HEAD 2>$null
    if ($LASTEXITCODE -eq 0 -and ($last -match '^packaging/windows/installer\.iss$')) { return $true }
  } catch {}
  return $false
}

$buildParams = @{}

$needInstaller = $false
if ($WithInstaller) {
  $needInstaller = $true
} elseif ($NoInstaller) {
  $needInstaller = $false
} elseif ($AutoInstaller -or (-not $WithInstaller -and -not $NoInstaller)) {
  # Fast build default: rebuild installer only when installer script changed.
  $needInstaller = (Test-InstallerScriptChanged)
}

if (-not $needInstaller) {
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
if ($needInstaller) {
  Write-Host "Fast build options: $echo (installer rebuild enabled)"
} else {
  Write-Host "Fast build options: $echo (exe-only incremental)"
}
& $buildScript @buildParams
