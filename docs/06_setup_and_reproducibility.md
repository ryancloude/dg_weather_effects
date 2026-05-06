# Setup and Reproducibility

## Purpose

This document explains how to run and deploy the project in a reproducible way.

It focuses on:

- required tools
- local Docker-based execution
- AWS deployment
- dashboard deployment

## Prerequisites

The main tools required are:

- Docker
- AWS CLI
- AWS credentials with access to the target account
- Node.js and the AWS CDK CLI
- Python 3.10+ for local scripts and CDK

The project is designed to run primarily in AWS, but individual jobs can also be built and tested locally with Docker.

## Configuration

The project reads configuration from:

- `.env`

The `.env.example` file includes the main settings used by the pipeline and monitoring layer, including:

- S3 bucket and DynamoDB table names
- AWS region
- Athena settings
- pipeline image tag
- schedule settings
- production training fingerprint
- monitoring email settings

In practice, `.env` is the main configuration file used when deploying the stacks.

## Local Execution

Each pipeline job has:

- its own Dockerfile
- its own container entrypoint
- a CLI-based runner

That means jobs can be built and run locally by passing normal command-line flags directly to the container.

### Example: event-page ingestion

```powershell
docker build -f .\docker\dockerfiles\Dockerfile.event_pages -t dgwe/ingest-pdga-event-pages:local .
docker run --rm dgwe/ingest-pdga-event-pages:local --incremental
```


## Container Entry Points

Each job image includes an entrypoint script.

The entrypoint scripts mainly do two things:

- run the correct job command when normal CLI arguments are passed in
- fall back to a help message if no arguments are provided

In other words, they are lightweight wrappers that make the job containers easier to run consistently.

## AWS Deployment

The AWS deployment flow has two main steps:

1. build and publish the job images to ECR
2. deploy the infrastructure stacks with CDK

## Publish pipeline images

The repo includes a PowerShell script for building and pushing the pipeline job images:

```powershell
powershell -File .\infra_cdk\scripts\publish_pipeline_images.ps1 -ImageTag latest
```

This script:

- resolves the AWS account and region
- logs Docker into ECR
- builds the job images from the repo Dockerfiles
- pushes the images to the job-specific ECR repositories

It can also be limited to a subset of jobs if needed.

## Deploy the stacks

The CDK app lives under `infra_cdk/`.

From that directory, the main deploy flow is:

```powershell
cd .\infra_cdk
cdk deploy dgwe-dev-Shared dgwe-dev-Orchestration
```

The stacks are split into:

- **Shared**
  - core resources such as S3, DynamoDB, ECS, ECR, Athena, and config parameters

- **Orchestration**
  - Step Functions, schedules, monitoring Lambda, and SES monitoring integration

## How the CDK app reads config

The CDK app loads settings from:

- `.env`
- shell environment variables

It also expects AWS account and region context from your configured AWS credentials.

## Dashboard Deployment

The dashboard is deployed separately from the AWS pipelines.

### Local dashboard entrypoint

The Streamlit wrapper is:

- `streamlit_app.py`

This imports the dashboard app from:

- `dashboard_weather_impacts/`

### Streamlit Cloud deployment

The dashboard is deployed through Streamlit Community Cloud and reads published reporting tables from S3.

The main deployment pieces are:

- repository branch
- main file path: `streamlit_app.py`
- Streamlit secrets for:
  - bucket name
  - region
  - AWS credentials

## Reproducibility Notes

A few things make the project easier to reproduce:

- containerized pipeline jobs
- explicit Dockerfiles per stage
- shared `.env`-driven configuration
- published ECR image workflow
- infrastructure defined in CDK
- DynamoDB checkpoints and run summaries
- replayable raw source data in S3

Together, these make it easier to rebuild the project environment and rerun the pipelines with the same general structure.

## Validation Checklist

A basic end-to-end validation flow looks like this:

1. confirm `.env` is populated
2. confirm AWS credentials are active
3. build and publish images to ECR
4. deploy the shared and orchestration stacks
5. trigger a pipeline run
6. confirm outputs are written to S3
7. confirm run summaries are written to DynamoDB
8. confirm monitoring email is delivered
9. confirm the dashboard reads the published reporting tables