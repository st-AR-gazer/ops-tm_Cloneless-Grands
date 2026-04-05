param(
    [string]$TaskName = "ClonelessGrandsWeekly",
    [ValidateSet("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")]
    [string]$Day = "SUN",
    [string]$Time = "18:05",
    [string[]]$RetryTimes = @("18:20", "20:20"),
    [string]$ConfigPath = "$PSScriptRoot\..\config.json",
    [string]$PythonExe = "python",
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"
Import-Module ScheduledTasks

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

function Parse-ClockTime {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    if ($Value -notmatch "^\d{2}:\d{2}$") {
        throw "Time must be in HH:mm format, e.g. 18:05"
    }

    return [datetime]::Today.Add([timespan]::Parse($Value))
}

$runnerAbsPath = (Resolve-Path $runnerPath).Path
$configAbsPath = (Resolve-Path $resolvedConfigPath).Path
$taskArgs = "-NoProfile -ExecutionPolicy Bypass -File `"$runnerAbsPath`" -ConfigPath `"$configAbsPath`" -PythonExe `"$PythonExe`" -ScheduledMode -ReleaseDay $Day -ReleaseTime 18:00 -ValidationLength 1"
$taskCommand = "powershell.exe $taskArgs"

$dayLookup = @{
    MON = "Monday"
    TUE = "Tuesday"
    WED = "Wednesday"
    THU = "Thursday"
    FRI = "Friday"
    SAT = "Saturday"
    SUN = "Sunday"
}

$scheduledTimes = @($Time) + @($RetryTimes)
$scheduledTimes = $scheduledTimes |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
    ForEach-Object { $_.Trim() } |
    Select-Object -Unique

if (-not $scheduledTimes -or $scheduledTimes.Count -eq 0) {
    throw "At least one schedule time is required."
}

$triggers = @()
foreach ($scheduledTime in $scheduledTimes) {
    $triggers += New-ScheduledTaskTrigger `
        -Weekly `
        -WeeksInterval 1 `
        -DaysOfWeek $dayLookup[$Day] `
        -At (Parse-ClockTime -Value $scheduledTime)
}

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $taskArgs
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Hours 72)

Write-Host "Creating/updating task '$TaskName'..."
Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $triggers `
    -Principal $principal `
    -Settings $settings `
    -Force | Out-Null

Write-Host "Task created."
Write-Host "Task: $TaskName"
Write-Host "Day:  $Day"
Write-Host "Times:"
foreach ($scheduledTime in $scheduledTimes) {
    Write-Host "  - $scheduledTime"
}
Write-Host "Run:  $taskCommand"

if ($RunNow) {
    Write-Host "Starting task immediately..."
    Start-ScheduledTask -TaskName $TaskName
}
