param(
  [switch]$NoInstaller,
  [switch]$SkipDeps,
  [switch]$SkipPyInstallerUpgrade,
  [switch]$NoClean,
  [switch]$SkipUiAssets
)

$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot\.."
Write-Host "Build flags => NoInstaller=$NoInstaller SkipDeps=$SkipDeps SkipPyInstallerUpgrade=$SkipPyInstallerUpgrade NoClean=$NoClean SkipUiAssets=$SkipUiAssets"

function Get-PythonExe {
  $candidates = @(
    "python",
    "C:\Users\soomin\AppData\Local\Programs\Python\Python311-arm64\python.exe"
  )
  foreach ($c in $candidates) {
    try {
      $cmd = Get-Command $c -ErrorAction SilentlyContinue
      if ($cmd) { return $cmd.Source }
      if (Test-Path $c) { return $c }
    } catch {}
  }
  throw "Python executable not found."
}

$py = Get-PythonExe

if (-not $SkipDeps) {
  & $py -m pip install -r requirements.txt
} else {
  Write-Host "Skipping dependency install (--SkipDeps)."
}

if (-not $SkipPyInstallerUpgrade) {
  & $py -m pip install --upgrade pyinstaller
} else {
  Write-Host "Skipping PyInstaller upgrade (--SkipPyInstallerUpgrade)."
}

if (-not $SkipUiAssets) {
  & $py "scripts\generate_ui_assets.py"
} else {
  Write-Host "Skipping UI asset generation (--SkipUiAssets)."
}

$pyInstallerArgs = @(
  "-m", "PyInstaller",
  "--noconfirm",
  "--onefile",
  "--windowed",
  "--name", "RezeroAgent",
  "--hidden-import", "tzdata",
  "--hidden-import", "numpy",
  "--collect-data", "tzdata",
  "--exclude-module", "matplotlib",
  "--exclude-module", "tkinter",
  "--exclude-module", "_tkinter",
  "--add-data", "ui\styles;ui/styles",
  "--add-data", "ui\themes;ui/themes",
  "--add-data", "ui\assets;ui/assets",
  "--add-data", "assets\fallback;assets/fallback",
  "main.py"
)
if (-not $NoClean) {
  $pyInstallerArgs += "--clean"
} else {
  Write-Host "Skipping clean build cache (--NoClean)."
}
& $py @pyInstallerArgs
Write-Host "NOTE: dist\\RezeroAgent.exe is for build verification. Use the installed app at C:\\Program Files\\RezeroAgent\\RezeroAgent.exe for production runs."

# version.txt (build date + commit hash), best-effort without external git command.
$commitHash = "nogit"
$gitHead = Join-Path (Get-Location) ".git\HEAD"
if (Test-Path $gitHead) {
  try {
    $headLine = (Get-Content $gitHead -TotalCount 1).Trim()
    if ($headLine -match "^ref:\s+(.+)$") {
      $refPath = Join-Path (Get-Location) ".git\$($Matches[1])"
      if (Test-Path $refPath) {
        $commitHash = (Get-Content $refPath -TotalCount 1).Trim()
      }
    } elseif ($headLine -match "^[0-9a-fA-F]{7,40}$") {
      $commitHash = $headLine
    }
  } catch {}
}
if ($commitHash -is [System.Array]) {
  $commitHash = "$($commitHash[0])".Trim()
} else {
  $commitHash = "$commitHash".Trim()
}
if ($commitHash -eq "nogit" -or -not $commitHash) {
  try {
    $mainHash = (Get-FileHash -Algorithm SHA1 -Path "main.py").Hash
    if ($mainHash) { $commitHash = "sha1-" + $mainHash.Substring(0,12).ToLower() }
  } catch {}
}
$buildStamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssK")
$versionLines = @(
  "build_date=$buildStamp",
  "commit=$commitHash"
)
Set-Content -Path "version.txt" -Value $versionLines -Encoding UTF8
if (Test-Path "dist\RezeroAgent.exe") {
  Set-Content -Path "dist\version.txt" -Value $versionLines -Encoding UTF8
}

if ($NoInstaller) {
  Write-Host "Skipping installer build (--NoInstaller)."
  exit 0
}

$isccCandidates = @(
  "C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
  "C:\Users\soomin\AppData\Local\Programs\Inno Setup 6\ISCC.exe"
)
$iscc = $isccCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if ($iscc) {
  & $iscc "packaging\windows\installer.iss"
  $desktop = [Environment]::GetFolderPath("Desktop")
  $installer = Join-Path (Get-Location) "dist\RezeroAgentInstaller.exe"
  if (Test-Path $installer) {
    Copy-Item $installer (Join-Path $desktop "RezeroAgentInstaller.exe") -Force
    Write-Host "Installer copied to Desktop: $desktop\\RezeroAgentInstaller.exe"
  }
} else {
  Write-Host "Inno Setup not found. Install it to build installer exe."
}
