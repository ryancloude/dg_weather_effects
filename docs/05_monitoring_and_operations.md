# Monitoring and Operations

## Purpose

This document describes how pipeline runs are monitored and how the project is operated day to day.

## Monitoring Overview

Both pipelines include automated post-run monitoring.

After a pipeline finishes:

1. Step Functions emits a status change event
2. EventBridge triggers the monitoring Lambda
3. the Lambda reads execution history, run summaries, and task metrics
4. it builds an HTML report
5. SES sends the report by email

This happens for both:

- the daily pipeline
- the weekly model refresh pipeline

## Monitoring Data Sources

The monitoring summary is built from several sources.

### Step Functions
Used for:

- pipeline status
- job order
- job start and end times
- total pipeline runtime
- shared `run_id`

### DynamoDB run summaries
Used for job-level counts such as:

- processed records or events
- failed records or events
- model error metrics where available

Each pipeline job writes its own run summary, which allows the monitor to pull job-level outputs into a single report.

### CloudWatch Container Insights
Used for:

- average CPU utilization
- peak CPU utilization
- average memory utilization
- peak memory utilization

These metrics are collected per ECS task.

### Athena usage signals
Used for:

- estimated Athena query cost for jobs that execute query-based reporting work

### Fargate sizing and runtime
Used for:

- estimated compute cost by job
- estimated total pipeline compute cost

## Metrics Included in the Monitoring Email

Each monitoring email includes:

- pipeline status
- pipeline start and end time
- total pipeline runtime
- runtime for each job
- processed counts by job
- failed counts by job
- `RMSE` and `MAE` for prediction runs
- average and peak CPU utilization
- average and peak memory utilization
- estimated Fargate cost by job
- estimated Athena cost by job
- total estimated pipeline cost

The report is sent as an HTML table so the full pipeline run can be reviewed in one place.

## Pipeline Status Rules

The monitoring layer reflects the final Step Functions execution result.

That means the pipeline is reported as:

- `SUCCEEDED` if the state machine completed successfully
- `FAILED` if the state machine failed
- other terminal states such as `TIMED_OUT` or `ABORTED` if they occur

The monitoring summary does not invent a separate “partial success” status.

## Operational Metadata

A shared `run_id` is passed into the ECS tasks by Step Functions and is used to tie together:

- job logs
- DynamoDB run summaries
- Step Functions execution history
- monitoring output

This is one of the main reasons the monitoring system can produce one coherent summary for a full pipeline run.

## Run Summaries Written by Jobs

Each major pipeline stage writes a run summary to DynamoDB.

This includes jobs such as:

- `ingest_pdga_event_pages`
- `ingest_pdga_live_results`
- `ingest_weather_observations`
- `silver_pdga_live_results`
- `silver_weather_observations`
- `silver_weather_enriched`
- `gold_wind_effects`
- `gold_wind_model_inputs`
- `train_round_wind_model`
- `score_round_wind_model`
- `report_round_weather_impacts`

Those summaries are what make the job-level monitoring table possible.

## Prediction Monitoring

The prediction job includes additional analytical metrics in its run summary:

- `RMSE`
- `MAE`
- metric row count

These are calculated from the scored round outputs and included in the monitoring email for prediction runs.

## Cost Estimation

The monitoring layer reports estimated cost rather than exact billed cost.

### Fargate estimate
Based on:

- allocated CPU
- allocated memory
- task runtime

### Athena estimate
Based on:

- bytes scanned during reporting-related query work

This is meant to provide operational visibility rather than billing-grade cost accounting.

## Operations Workflow

A typical operational workflow looks like this:

1. a scheduled pipeline runs in Step Functions
2. ECS jobs execute in sequence
3. each job writes checkpoints and run summaries
4. the state machine finishes
5. the monitoring Lambda sends a summary email
6. if something failed, the run can be traced through:
   - Step Functions execution history
   - CloudWatch logs
   - DynamoDB run summaries and checkpoints

## Debugging a Failed Run

When a run fails, the main places to inspect are:

- **Step Functions**
  - to find which job failed and in what order

- **CloudWatch logs**
  - to inspect the job-level error details

- **DynamoDB run summaries**
  - to see counts, status, and failure context written by the jobs

- **monitoring email**
  - to quickly identify which step failed, how far the run got, and what the resource profile looked like

## Why This Layer Matters

The monitoring layer makes the project easier to operate because it turns pipeline execution into something visible and reviewable.

Instead of relying only on raw logs, each run produces a structured summary of:

- what ran
- how long it took
- how much data was processed
- whether prediction quality changed
- how much compute and query cost the run likely used