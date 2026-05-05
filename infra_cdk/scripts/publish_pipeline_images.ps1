[CmdletBinding()]
param(
    [string]$Region = $(if ($env:AWS_REGION) { $env:AWS_REGION } elseif ($env:CDK_DEFAULT_REGION) { $env:CDK_DEFAULT_REGION } else { "us-east-2" }),
    [string]$AccountId = "",
    [string]$ImageTag = "latest",
    [string]$AwsProfile = "",
    [string[]]$IncludeJobs = @(),
    [switch]$SkipDockerLogin
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Section {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Invoke-AwsCliJson {
    param(
        [string[]]$Arguments
    )

    $awsArgs = @()
    if ($AwsProfile) {
        $awsArgs += @("--profile", $AwsProfile)
    }
    $awsArgs += $Arguments

    $output = & aws @awsArgs
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI command failed: aws $($awsArgs -join ' ')"
    }

    return $output
}

$scriptDir = $PSScriptRoot
if (-not $scriptDir) {
    throw "Unable to resolve PSScriptRoot for publish_pipeline_images.ps1."
}

$infraDir = Split-Path -Parent $scriptDir
$repoRoot = Split-Path -Parent $infraDir

Set-Location $repoRoot

$jobs = @(
    @{
        JobName    = "ingest_pdga_event_pages"
        Repository = "dgwe/ingest-pdga-event-pages"
        Dockerfile = "docker/dockerfiles/Dockerfile.event_pages"
    },
    @{
        JobName    = "ingest_pdga_live_results"
        Repository = "dgwe/ingest-pdga-live-results"
        Dockerfile = "docker/dockerfiles/Dockerfile.live_results"
    },
    @{
        JobName    = "ingest_weather_observations"
        Repository = "dgwe/ingest-weather-observations"
        Dockerfile = "docker/dockerfiles/Dockerfile.weather_observations"
    },
    @{
        JobName    = "silver_pdga_live_results"
        Repository = "dgwe/silver-pdga-live-results"
        Dockerfile = "docker/dockerfiles/Dockerfile.silver_live_results"
    },
    @{
        JobName    = "silver_weather_observations"
        Repository = "dgwe/silver-weather-observations"
        Dockerfile = "docker/dockerfiles/Dockerfile.silver_weather_observations"
    },
    @{
        JobName    = "silver_weather_enriched"
        Repository = "dgwe/silver-weather-enriched"
        Dockerfile = "docker/dockerfiles/Dockerfile.silver_weather_enriched"
    },
    @{
        JobName    = "gold_wind_effects"
        Repository = "dgwe/gold-wind-effects"
        Dockerfile = "docker/dockerfiles/Dockerfile.gold_wind_effects"
    },
    @{
        JobName    = "gold_wind_model_inputs"
        Repository = "dgwe/gold-wind-model-inputs"
        Dockerfile = "docker/dockerfiles/Dockerfile.gold_wind_model_inputs"
    },
    @{
        JobName    = "train_round_wind_model"
        Repository = "dgwe/train-round-wind-model"
        Dockerfile = "docker/dockerfiles/Dockerfile.train_round_wind_model"
    },
    @{
        JobName    = "score_round_wind_model"
        Repository = "dgwe/score-round-wind-model"
        Dockerfile = "docker/dockerfiles/Dockerfile.score_round_wind_model"
    },
    @{
        JobName    = "report_round_weather_impacts"
        Repository = "dgwe/report-round-weather-impacts"
        Dockerfile = "docker/dockerfiles/Dockerfile.report_round_weather_impacts"
    }
)

if ($IncludeJobs.Count -gt 0) {
    $requested = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($name in $IncludeJobs) {
        [void]$requested.Add($name)
    }

    $jobs = @($jobs | Where-Object { $requested.Contains($_.JobName) })

    if ($jobs.Count -eq 0) {
        throw "No jobs matched IncludeJobs: $($IncludeJobs -join ', ')"
    }
}

if (-not $AccountId) {
    Write-Section "Resolving AWS account ID"
    $callerIdentityRaw = Invoke-AwsCliJson -Arguments @("sts", "get-caller-identity", "--output", "json")
    $callerIdentity = $callerIdentityRaw | ConvertFrom-Json
    $AccountId = $callerIdentity.Account
}

$registry = "$AccountId.dkr.ecr.$Region.amazonaws.com"

Write-Section "Publish configuration"
Write-Host "Repo root : $repoRoot"
Write-Host "Region    : $Region"
Write-Host "Account   : $AccountId"
Write-Host "Registry  : $registry"
Write-Host "Image tag : $ImageTag"
Write-Host "Jobs      : $($jobs.JobName -join ', ')"

if (-not $SkipDockerLogin) {
    Write-Section "Logging Docker into ECR"
    $loginArgs = @()
    if ($AwsProfile) {
        $loginArgs += @("--profile", $AwsProfile)
    }
    $loginArgs += @("ecr", "get-login-password", "--region", $Region)

    $password = & aws @loginArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to retrieve ECR login password."
    }

    $password | docker login --username AWS --password-stdin $registry
    if ($LASTEXITCODE -ne 0) {
        throw "Docker login to ECR failed."
    }
}

foreach ($job in $jobs) {
    $dockerfilePath = Join-Path $repoRoot $job.Dockerfile
    if (-not (Test-Path $dockerfilePath)) {
        throw "Dockerfile not found for job '$($job.JobName)': $dockerfilePath"
    }

    $imageUri = "$registry/$($job.Repository):$ImageTag"

    Write-Section "Building $($job.JobName)"
    Write-Host "Dockerfile: $dockerfilePath"
    Write-Host "Image URI : $imageUri"

    & docker build `
        --file $dockerfilePath `
        --tag $imageUri `
        $repoRoot

    if ($LASTEXITCODE -ne 0) {
        throw "Docker build failed for job '$($job.JobName)'."
    }

    Write-Section "Pushing $($job.JobName)"
    & docker push $imageUri

    if ($LASTEXITCODE -ne 0) {
        throw "Docker push failed for job '$($job.JobName)'."
    }
}

Write-Section "Done"
Write-Host "Successfully built and pushed $($jobs.Count) image(s) with tag '$ImageTag'." -ForegroundColor Green
