param(
    [Parameter(Mandatory = $true)]
    [string]$InputPath,
    [Parameter(Mandatory = $true)]
    [string]$OutputPath,
    [Parameter(Mandatory = $true)]
    [string]$NewUid,
    [string]$ExePath = "",
    [string]$TemplatePath = "",
    [string]$OutputSuffix = "uidpatched"
)

$ErrorActionPreference = "Stop"

if (-not $ExePath) {
    $ExePath = Join-Path $PSScriptRoot "GbxJsonEditor.Cli.exe"
}

if (-not $TemplatePath) {
    $TemplatePath = Join-Path $PSScriptRoot "uid-rewrite.instructions.template.json"
}

if (-not (Test-Path $ExePath)) {
    throw "GbxJsonEditor executable not found: $ExePath"
}

if (-not (Test-Path $TemplatePath)) {
    throw "Instructions template not found: $TemplatePath"
}

if (-not (Test-Path $InputPath)) {
    throw "Input file not found: $InputPath"
}

$templateContent = Get-Content -Path $TemplatePath -Raw -Encoding UTF8
$instructionsContent = $templateContent.Replace("__NEW_UID__", $NewUid)

$tempInstructions = Join-Path ([System.IO.Path]::GetTempPath()) ("uid-rewrite-" + [guid]::NewGuid().ToString("N") + ".json")
$instructionsContent | Out-File -FilePath $tempInstructions -Encoding UTF8

try {
    & $ExePath $InputPath $tempInstructions $OutputSuffix
    if ($LASTEXITCODE -ne 0) {
        throw "GbxJsonEditor.Cli.exe failed with exit code $LASTEXITCODE"
    }

    $inFileName = [System.IO.Path]::GetFileName($InputPath)
    if ($inFileName.EndsWith(".Map.Gbx", [System.StringComparison]::OrdinalIgnoreCase)) {
        $baseName = $inFileName.Substring(0, $inFileName.Length - ".Map.Gbx".Length)
    } else {
        $baseName = [System.IO.Path]::GetFileNameWithoutExtension($inFileName)
    }

    $inputParent = Split-Path -Path $InputPath -Parent
    $generatedPath = Join-Path $inputParent ($baseName + "_" + $OutputSuffix + ".Map.Gbx")

    if (-not (Test-Path $generatedPath)) {
        throw "Expected generated file not found: $generatedPath"
    }

    $outputParent = Split-Path -Path $OutputPath -Parent
    if ($outputParent -and -not (Test-Path $outputParent)) {
        New-Item -ItemType Directory -Path $outputParent -Force | Out-Null
    }

    Move-Item -Path $generatedPath -Destination $OutputPath -Force
}
finally {
    if (Test-Path $tempInstructions) {
        Remove-Item -Path $tempInstructions -Force
    }
}
