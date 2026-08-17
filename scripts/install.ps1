<#
.SYNOPSIS
    Sutra CLI installer for Windows.

.DESCRIPTION
    irm https://raw.githubusercontent.com/startr-trade/sutra/main/scripts/install.ps1 | iex

    Downloads the `sutra.exe` binary for this platform from a GitHub release, VERIFIES its
    SHA-256 against the release's own SHA256SUMS, and installs it. Nothing else: no PATH is
    rewritten without telling you, no service is registered.

.PARAMETER Version
    Release tag to install (e.g. v0.2.0-rc.1). Default: the newest release, pre-releases
    included. Also settable as $env:SUTRA_VERSION.

.PARAMETER Dir
    Install directory. Default: $env:LOCALAPPDATA\Programs\sutra\bin. Also $env:SUTRA_INSTALL_DIR.

.PARAMETER NoVerify
    Skip checksum verification (discouraged).
#>
[CmdletBinding()]
param(
    [string]$Version = $env:SUTRA_VERSION,
    [string]$Dir     = $env:SUTRA_INSTALL_DIR,
    [switch]$NoVerify
)

$ErrorActionPreference = 'Stop'
$repo = 'startr-trade/sutra'
$dl   = "https://github.com/$repo/releases/download"

# Only the x86_64 MSVC target is published for Windows (see .github/workflows/release.yml).
if ([System.Environment]::Is64BitOperatingSystem -ne $true -or
    $env:PROCESSOR_ARCHITECTURE -eq 'ARM64') {
    throw "sutra-install: no published Windows asset for this architecture ($env:PROCESSOR_ARCHITECTURE). Build from source: cargo install --path rust/crates/sutra-cli"
}
$target = 'x86_64-pc-windows-msvc'

# THREE sources, tried in order, because each one has a failure mode the next covers:
#   1. /releases/latest  — right once a STABLE release exists; 404 while every release is a
#      pre-release, which every 0.x has been;
#   2. the release LIST  — includes pre-releases, and has been seen answering an EMPTY ARRAY
#      while the release was perfectly fetchable by tag;
#   3. git TAGS          — a different endpoint, which answered when the list did not. Newest
#      v-prefixed tag first, then confirmed to carry a release.
# GH_TOKEN / GITHUB_TOKEN, when present, lifts api.github.com from 60 requests an hour to 5000.
$api = "https://api.github.com/repos/$repo"
$ghHeaders = @{ 'User-Agent' = 'sutra-install' }
$ghToken = if ($env:GH_TOKEN) { $env:GH_TOKEN } else { $env:GITHUB_TOKEN }
if ($ghToken) { $ghHeaders['Authorization'] = "Bearer $ghToken" }

function Get-Json($uri) {
    try { Invoke-RestMethod -Uri $uri -Headers $ghHeaders -TimeoutSec 60 } catch { $null }
}

if (-not $Version) {
    Write-Host '  resolving the latest release...'
    $latest = Get-Json "$api/releases/latest"
    if ($latest -and $latest.tag_name) { $Version = $latest.tag_name }
    if (-not $Version) {
        $rel = Get-Json "$api/releases?per_page=1"
        if ($rel -and $rel.Count -gt 0) { $Version = $rel[0].tag_name }
    }
    if (-not $Version) {
        Write-Host '  release list empty - falling back to tags'
        $tags = Get-Json "$api/tags?per_page=100"
        foreach ($t in ($tags | Where-Object { $_.name -like 'v*' } |
                        Sort-Object -Property @{ Expression = { $_.name } } -Descending)) {
            if (Get-Json "$api/releases/tags/$($t.name)") { $Version = $t.name; break }
        }
    }
    if (-not $Version) {
        throw "sutra-install: could not resolve a release tag from $api (rate-limited, or nothing published). Pin one with -Version v0.2.0-rc.1, or set GH_TOKEN."
    }
}
Write-Host "  version: $Version"

if (-not $Dir) { $Dir = Join-Path $env:LOCALAPPDATA 'Programs\sutra\bin' }
New-Item -ItemType Directory -Force -Path $Dir | Out-Null

$asset = "sutra-$Version-$target.zip"
$tmp   = Join-Path ([System.IO.Path]::GetTempPath()) ("sutra-install-" + [guid]::NewGuid())
New-Item -ItemType Directory -Force -Path $tmp | Out-Null

try {
    Write-Host "  downloading $asset..."
    $archive = Join-Path $tmp $asset
    Invoke-WebRequest -Uri "$dl/$Version/$asset" -OutFile $archive -UseBasicParsing -TimeoutSec 300

    if ($NoVerify) {
        Write-Host '  checksum verification SKIPPED (-NoVerify)'
    } else {
        $sumsPath = Join-Path $tmp 'SHA256SUMS'
        Invoke-WebRequest -Uri "$dl/$Version/SHA256SUMS" -OutFile $sumsPath -UseBasicParsing -TimeoutSec 300
        $want = (Select-String -Path $sumsPath -Pattern ([regex]::Escape($asset)) |
                 Select-Object -First 1).Line -split '\s+' | Select-Object -First 1
        if (-not $want) { throw "sutra-install: SHA256SUMS carries no entry for $asset" }
        $got = (Get-FileHash -Path $archive -Algorithm SHA256).Hash.ToLower()
        if ($want.ToLower() -ne $got) {
            throw "sutra-install: CHECKSUM MISMATCH for $asset`n  expected $want`n  got      $got`nDo not use this download."
        }
        Write-Host '  checksum ok'
    }

    Expand-Archive -Path $archive -DestinationPath $tmp -Force
    $exe = Get-ChildItem -Path $tmp -Filter 'sutra.exe' -Recurse | Select-Object -First 1
    if (-not $exe) { throw 'sutra-install: the archive did not contain sutra.exe' }
    Copy-Item -Path $exe.FullName -Destination (Join-Path $Dir 'sutra.exe') -Force
    Write-Host "  installed $Dir\sutra.exe"
} finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}

& (Join-Path $Dir 'sutra.exe') --version

# PATH is a user-visible change, so it is offered rather than done silently.
$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if ($userPath -notlike "*$Dir*") {
    Write-Host ""
    Write-Host "$Dir is not on your PATH. Add it for this user with:"
    Write-Host "    [Environment]::SetEnvironmentVariable('Path', `"`$env:Path;$Dir`", 'User')"
}
Write-Host ""
Write-Host 'Next:  sutra create app my-first-app     # then see https://sutra.startr.trade'
