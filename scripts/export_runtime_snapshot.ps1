param(
  [string]$AppDataRoot = "$env:APPDATA\RezeroAgent",
  [string]$OutRoot = "reports\runtime_snapshot\latest",
  [int]$TailLinesLargeLog = 4000,
  [int]$LargeLogThresholdMB = 5
)

$ErrorActionPreference = "Stop"

function Ensure-Dir([string]$Path) {
  New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

function Write-Text([string]$Path, [string]$Text) {
  $parent = Split-Path -Parent $Path
  if ($parent) { Ensure-Dir $parent }
  Set-Content -Path $Path -Value $Text -Encoding UTF8
}

function Copy-Safe([string]$Source, [string]$Target) {
  $parent = Split-Path -Parent $Target
  if ($parent) { Ensure-Dir $parent }
  Copy-Item -Path $Source -Destination $Target -Force
}

if (-not (Test-Path $AppDataRoot)) {
  throw "AppData runtime path not found: $AppDataRoot"
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$outAbs = Join-Path $repoRoot $OutRoot
$logsOut = Join-Path $outAbs "logs"
$dbOut = Join-Path $outAbs "db"
$metaOut = Join-Path $outAbs "meta"

if (Test-Path $outAbs) {
  Remove-Item -Recurse -Force $outAbs
}

Ensure-Dir $outAbs
Ensure-Dir $logsOut
Ensure-Dir $dbOut
Ensure-Dir $metaOut

$utcNow = [DateTime]::UtcNow.ToString("o")

$storageRoot = Join-Path $AppDataRoot "storage"
$logsRoot = Join-Path $storageRoot "logs"

$manifest = [System.Collections.Generic.List[string]]::new()
$manifest.Add("# Runtime Snapshot Manifest")
$manifest.Add("")
$manifest.Add("- generated_utc: $utcNow")
$manifest.Add("- appdata_root: $AppDataRoot")
$manifest.Add("- output_root: $outAbs")
$manifest.Add("")

$manifest.Add("## Storage Directories")
if (Test-Path $storageRoot) {
  Get-ChildItem -Path $storageRoot -Force | ForEach-Object {
    $manifest.Add("- $($_.Name) [$($_.Mode)]")
  }
} else {
  $manifest.Add("- (missing) $storageRoot")
}
$manifest.Add("")

$manifest.Add("## Log Files (source)")
if (Test-Path $logsRoot) {
  $logFiles = Get-ChildItem -Path $logsRoot -File | Sort-Object Length -Descending
  foreach ($f in $logFiles) {
    $sizeMb = [Math]::Round(($f.Length / 1MB), 2)
    $manifest.Add("- $($f.Name) : $sizeMb MB")
  }

  foreach ($f in $logFiles) {
    $target = Join-Path $logsOut $f.Name
    $sizeMb = ($f.Length / 1MB)
    if ($sizeMb -gt $LargeLogThresholdMB) {
      # Keep tail for very large logs to keep repo usable.
      Get-Content -Path $f.FullName -Tail $TailLinesLargeLog | Set-Content -Path $target -Encoding UTF8
      Add-Content -Path (Join-Path $metaOut "snapshot_notes.txt") -Value "TAILED $($f.Name): size=${sizeMb}MB tail=$TailLinesLargeLog"
    } else {
      Copy-Safe $f.FullName $target
    }
  }
} else {
  $manifest.Add("- (missing) $logsRoot")
}

$manifest.Add("")
$manifest.Add("## Included Config")
$repoSettings = Join-Path $repoRoot "config\settings.yaml"
if (Test-Path $repoSettings) {
  Copy-Safe $repoSettings (Join-Path $metaOut "settings.yaml")
  $manifest.Add("- repo config/settings.yaml")
}

# AppData config: exclude known secret/token files
$appConfig = Join-Path $AppDataRoot "config"
if (Test-Path $appConfig) {
  Ensure-Dir (Join-Path $metaOut "appdata_config")
  Get-ChildItem -Path $appConfig -File | ForEach-Object {
    $name = $_.Name.ToLowerInvariant()
    if (
      $name -like "*token*" -or
      $name -like "*secret*" -or
      $name -eq "client_secrets.json" -or
      $name -eq "blogger_token.json" -or
      $name -eq "service_account.json"
    ) {
      Add-Content -Path (Join-Path $metaOut "redacted_files.txt") -Value $_.Name
      return
    }
    Copy-Safe $_.FullName (Join-Path $metaOut "appdata_config\$($_.Name)")
  }
  $manifest.Add("- appdata config/* (secret/token files redacted)")
}

$manifest.Add("")
$manifest.Add("## DB Summary")
$dbFiles = @(
  (Join-Path $logsRoot "agent_logs.sqlite3"),
  (Join-Path $storageRoot "posts_index.sqlite"),
  (Join-Path $storageRoot "keywords.sqlite")
)

$dbSummaryPath = Join-Path $dbOut "DATABASE_SUMMARY.md"
$dbSummary = [System.Collections.Generic.List[string]]::new()
$dbSummary.Add("# Database Summary")
$dbSummary.Add("")
$dbSummary.Add("- generated_utc: $utcNow")
$dbSummary.Add("")

foreach ($db in $dbFiles) {
  if (Test-Path $db) {
    $name = [System.IO.Path]::GetFileName($db)
    Copy-Safe $db (Join-Path $dbOut $name)
    $dbSummary.Add("## $name")
    $dbSummary.Add("")
    $py = @"
import sqlite3, json, pathlib
db = pathlib.Path(r'''$db''')
out = []
conn = sqlite3.connect(str(db))
cur = conn.cursor()
tables = [r[0] for r in cur.execute("select name from sqlite_master where type='table' order by name").fetchall()]
for t in tables:
    try:
        c = cur.execute(f"select count(*) from [{t}]").fetchone()[0]
    except Exception:
        c = "n/a"
    out.append((t, c))
conn.close()
print(json.dumps(out, ensure_ascii=False))
"@
    $json = @($py | python -) -join ""
    try {
      $rows = $json | ConvertFrom-Json
      foreach ($r in $rows) {
        $dbSummary.Add("- $($r[0]) : $($r[1]) rows")
      }
    } catch {
      $dbSummary.Add("- (failed to parse table counts)")
    }
    $dbSummary.Add("")
  }
}

Write-Text $dbSummaryPath ($dbSummary -join "`r`n")
Write-Text (Join-Path $outAbs "MANIFEST.md") ($manifest -join "`r`n")

# Quick map for GPT
$map = @"
# GPT Runtime Map

This folder is a runtime snapshot from `%APPDATA%\RezeroAgent` for GPT analysis.

## Key Files
- `MANIFEST.md`: snapshot timestamp, included items, source log sizes
- `logs/*.jsonl`: execution/QA/image/publish logs (large logs are tail-only)
- `db/DATABASE_SUMMARY.md`: sqlite table/row summary
- `db/*.sqlite`: runtime DB copies
- `meta/settings.yaml`: active runtime settings
- `meta/appdata_config/*`: AppData config copy (secret/token files redacted)

## Log Purpose
- `logs/workflow_perf.jsonl`: stage timing and bottlenecks
- `logs/qa_timing.jsonl`: QA check timing breakdown
- `logs/visual_pipeline.jsonl`: image generation retries/failures
- `logs/publisher_upload.jsonl`: Blogger image upload responses
- `logs/thumbnail_gate.jsonl`: thumbnail preflight gate reasons
- `logs/agent_events.jsonl`: run-level result events
- `logs/ollama_calls.jsonl`: local LLM usage/fallback trace
"@
Write-Text (Join-Path $outAbs "README.md") $map

Write-Host "Runtime snapshot exported: $outAbs"
