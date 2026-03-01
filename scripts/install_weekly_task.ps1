param(
    [string]$TaskName = "ClonelessGrandsWeekly",
    [ValidateSet("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")]
    [string]$Day = "MON",
    [string]$Time = "18:00",
    [string]$ConfigPath = "$PSScriptRoot\..\config.json",
    [string]$PythonExe = "python",
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

$runnerPath = Join-Path $PSScriptRoot "run_cloneless_grands.ps1"
if (-not (Test-Path $runnerPath)) {
    throw "Could not find runner script at: $runnerPath"
}

$resolvedConfigPath = $ConfigPath
if (-not [System.IO.Path]::IsPathRooted($resolvedConfigPath)) {
    $resolvedConfigPath = Join-Path $repoRoot $resolvedConfigPath
}
if (-not (Test-Path $resolvedConfigPath)) {
    throw "Could not find config file at: $resolvedConfigPath"
}

if ($Time -notmatch "^\d{2}:\d{2}$") {
    throw "Time must be in HH:mm format, e.g. 18:00"
}

$runnerAbsPath = (Resolve-Path $runnerPath).Path
$configAbsPath = (Resolve-Path $resolvedConfigPath).Path
$taskCommand = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$runnerAbsPath`" -ConfigPath `"$configAbsPath`" -PythonExe `"$PythonExe`""

Write-Host "Creating/updating task '$TaskName'..."
schtasks /Create /TN $TaskName /SC WEEKLY /D $Day /ST $Time /TR $taskCommand /F | Out-Host
if ($LASTEXITCODE -ne 0) {
    throw "Failed to create scheduled task. schtasks exit code: $LASTEXITCODE"
}

Write-Host "Task created."
Write-Host "Task: $TaskName"
Write-Host "Day:  $Day"
Write-Host "Time: $Time"
Write-Host "Run:  $taskCommand"

if ($RunNow) {
    Write-Host "Starting task immediately..."
    schtasks /Run /TN $TaskName | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Task created, but immediate run failed. schtasks exit code: $LASTEXITCODE"
    }
}
