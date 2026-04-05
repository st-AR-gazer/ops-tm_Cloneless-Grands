param(
    [string]$ConfigPath = "$PSScriptRoot\..\config.json",
    [string]$PythonExe = "python",
    [string[]]$ExtraArgs = @()
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $repoRoot

$scriptPath = Join-Path $repoRoot "src\cloneless_grands.py"
if (-not (Test-Path $scriptPath)) {
    throw "Could not find script at: $scriptPath"
}

$resolvedConfigPath = $ConfigPath
if (-not [System.IO.Path]::IsPathRooted($resolvedConfigPath)) {
    $resolvedConfigPath = Join-Path $repoRoot $resolvedConfigPath
}
if (-not (Test-Path $resolvedConfigPath)) {
    throw "Could not find config at: $resolvedConfigPath"
}

& $PythonExe $scriptPath --config $resolvedConfigPath --check-compliance @ExtraArgs
if ($LASTEXITCODE -ne 0) {
    throw "cloneless_grands.py compliance check exited with code $LASTEXITCODE"
}
