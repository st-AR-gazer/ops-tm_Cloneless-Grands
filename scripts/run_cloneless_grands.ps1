param(
    [string]$ConfigPath = "$PSScriptRoot\..\config.json",
    [string]$PythonExe = "python",
    [string[]]$ExtraArgs = @(),
    [switch]$ScheduledMode,
    [ValidateSet("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")]
    [string]$ReleaseDay = "SUN",
    [string]$ReleaseTime = "18:00",
    [int]$ValidationLength = 1
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

function Invoke-PythonScript {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [switch]$CaptureOutput
    )

    if ($CaptureOutput) {
        $output = & $PythonExe $scriptPath @Arguments
        return [pscustomobject]@{
            ExitCode = $LASTEXITCODE
            Output   = @($output)
        }
    }

    & $PythonExe $scriptPath @Arguments
    return [pscustomobject]@{
        ExitCode = $LASTEXITCODE
        Output   = @()
    }
}

function Parse-ClockTime {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    if ($Value -notmatch "^\d{2}:\d{2}$") {
        throw "ReleaseTime must be in HH:mm format, e.g. 18:00"
    }

    return [timespan]::Parse($Value)
}

function Get-ExpectedWeeklyReleaseDate {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ReleaseDayCode,
        [Parameter(Mandatory = $true)]
        [timespan]$ReleaseClockTime
    )

    $dayLookup = @{
        MON = [System.DayOfWeek]::Monday
        TUE = [System.DayOfWeek]::Tuesday
        WED = [System.DayOfWeek]::Wednesday
        THU = [System.DayOfWeek]::Thursday
        FRI = [System.DayOfWeek]::Friday
        SAT = [System.DayOfWeek]::Saturday
        SUN = [System.DayOfWeek]::Sunday
    }

    $now = Get-Date
    $expectedDate = $now
    if ($now.DayOfWeek -eq $dayLookup[$ReleaseDayCode] -and $now.TimeOfDay -ge $ReleaseClockTime) {
        $expectedDate = $now.AddDays(1)
    }
    return $expectedDate
}

function Get-IsoWeekYear {
    param(
        [Parameter(Mandatory = $true)]
        [datetime]$Date
    )

    $dayOffset = ([int][System.DayOfWeek]::Thursday - [int]$Date.DayOfWeek + 7) % 7
    $thursday = $Date.AddDays($dayOffset)
    $isoYear = $thursday.Year
    $calendar = [System.Globalization.CultureInfo]::InvariantCulture.Calendar
    $isoWeek = $calendar.GetWeekOfYear(
        $thursday,
        [System.Globalization.CalendarWeekRule]::FirstFourDayWeek,
        [System.DayOfWeek]::Monday
    )

    return [pscustomobject]@{
        Year = $isoYear
        Week = $isoWeek
    }
}

if (-not $ScheduledMode) {
    $runArgs = @("--config", $resolvedConfigPath) + $ExtraArgs
    $result = Invoke-PythonScript -Arguments $runArgs
    if ($result.ExitCode -ne 0) {
        throw "cloneless_grands.py exited with code $($result.ExitCode)"
    }
    exit 0
}

$releaseClockTime = Parse-ClockTime -Value $ReleaseTime
$expectedDate = Get-ExpectedWeeklyReleaseDate -ReleaseDayCode $ReleaseDay -ReleaseClockTime $releaseClockTime
$expectedIso = Get-IsoWeekYear -Date $expectedDate
$expectedIsoYear = [int]$expectedIso.Year
$expectedIsoWeek = [int]$expectedIso.Week

Write-Host "Scheduled mode enabled."
Write-Host "Expected release window: ISO year $expectedIsoYear week $expectedIsoWeek"

$latestArgs = @("--config", $resolvedConfigPath, "--length", "1", "--print-latest-weekly")
$latestResult = Invoke-PythonScript -Arguments $latestArgs -CaptureOutput
if ($latestResult.ExitCode -ne 0) {
    throw "Failed to query latest weekly campaign. Exit code: $($latestResult.ExitCode)"
}

$latestJson = ($latestResult.Output -join [Environment]::NewLine).Trim()
if (-not $latestJson) {
    throw "Latest weekly campaign query returned no output."
}

$jsonStart = $latestJson.IndexOf('{')
if ($jsonStart -lt 0) {
    throw "Latest weekly campaign query did not return a JSON payload."
}
$latestJson = $latestJson.Substring($jsonStart)

$latestWeekly = $latestJson | ConvertFrom-Json
$latestWeek = [int]$latestWeekly.week
$latestYear = [int]$latestWeekly.year

Write-Host "Latest Weekly Grand from API: year=$latestYear week=$latestWeek name=$($latestWeekly.campaign_name)"

if ($latestWeek -ne $expectedIsoWeek -or $latestYear -ne $expectedIsoYear) {
    Write-Host "Latest Weekly Grand is not yet the expected newly released week. Leaving retries enabled."
    exit 1
}

$complianceArgs = @("--config", $resolvedConfigPath, "--check-compliance", "--length", $ValidationLength.ToString())
$preCompliance = Invoke-PythonScript -Arguments $complianceArgs
if ($preCompliance.ExitCode -eq 0) {
    Write-Host "Latest Weekly Grand already validates successfully. Skipping scheduled publish."
    exit 0
}

Write-Host "Latest Weekly Grand is not yet validated. Running publish pipeline..."
$runArgs = @("--config", $resolvedConfigPath) + $ExtraArgs
$runResult = Invoke-PythonScript -Arguments $runArgs
if ($runResult.ExitCode -ne 0) {
    Write-Host "Publish run failed. Leaving retries enabled."
    exit $runResult.ExitCode
}

Write-Host "Running post-publish compliance validation..."
$postCompliance = Invoke-PythonScript -Arguments $complianceArgs
if ($postCompliance.ExitCode -ne 0) {
    Write-Host "Post-publish compliance still failing. Leaving retries enabled."
    exit 1
}

Write-Host "Post-publish compliance passed. Later retry triggers should no-op."
