# Pipeline Jobs

## Purpose

This document summarizes the main pipeline jobs, what each one does, and how they fit into the daily and weekly workflows.

It is meant to be a practical map of the pipeline, not a line-by-line code walkthrough.

## Pipeline Overview

The project uses two orchestrated workflows:

- **Daily pipeline**
  - pulls new source data
  - updates downstream datasets incrementally
  - generates predictions for newly available data
  - republishes dashboard reporting outputs

- **Weekly model refresh pipeline**
  - retrains the round-level model
  - reruns downstream prediction and reporting steps

## Daily Pipeline

The current daily pipeline runs these jobs in order:

1. `ingest_pdga_event_pages`
2. `ingest_pdga_live_results`
3. `silver_pdga_live_results`
4. `ingest_weather_observations`
5. `silver_weather_observations`
6. `silver_weather_enriched`
7. `gold_wind_effects`
8. `gold_wind_model_inputs`
9. `score_round_wind_model`
10. `report_round_weather_impacts`

## Weekly Model Refresh Pipeline

The current weekly pipeline runs these jobs in order:

1. `train_round_wind_model`
2. `score_round_wind_model`
3. `report_round_weather_impacts`

## Job Summaries

## `ingest_pdga_event_pages`

**Purpose**
- collects PDGA event metadata from event pages

**Main inputs**
- PDGA event HTML

**Main outputs**
- raw HTML in S3
- parsed event metadata in DynamoDB
- event-page run summaries

**Role in the pipeline**
- drives event discovery and provides event dates, location, status, and division/round structure for downstream jobs

## `ingest_pdga_live_results`

**Purpose**
- collects PDGA live-results payloads by event, division, and round

**Main inputs**
- discovered event metadata and division/round structure

**Main outputs**
- raw live-results JSON in S3
- fetch state in DynamoDB
- run summaries in DynamoDB

**Role in the pipeline**
- provides the raw competition results used by the Silver live-results transform

## `silver_pdga_live_results`

**Purpose**
- converts raw live-results payloads into cleaned round-level and hole-level datasets

**Main inputs**
- Bronze live-results payloads
- event metadata and state

**Main outputs**
- `player_rounds`
- `player_holes`
- event checkpoints
- run summaries
- quarantine outputs for data-quality failures

**Role in the pipeline**
- creates the main competition-results datasets used throughout the rest of the project

## `ingest_weather_observations`

**Purpose**
- collects historical weather observations aligned to event timing and location

**Main inputs**
- event metadata
- round timing context
- location / geocode information

**Main outputs**
- raw weather JSON in S3
- weather fetch state in DynamoDB
- weather run summaries

**Role in the pipeline**
- provides the raw weather inputs used for downstream normalization and enrichment

## `silver_weather_observations`

**Purpose**
- standardizes raw weather responses into cleaned observation datasets

**Main inputs**
- Bronze weather payloads

**Main outputs**
- cleaned weather observation datasets
- event checkpoints
- run summaries
- quarantine outputs for data-quality failures

**Role in the pipeline**
- creates the canonical weather layer used for downstream joins

## `silver_weather_enriched`

**Purpose**
- joins cleaned weather data back onto round-level and hole-level competition results

**Main inputs**
- Silver live-results outputs
- Silver weather observations

**Main outputs**
- weather-enriched round datasets
- weather-enriched hole datasets
- event checkpoints
- run summaries

**Role in the pipeline**
- connects weather context to competition outcomes and prepares the project for Gold-layer analysis

## `gold_wind_effects`

**Purpose**
- creates analysis-ready weather effect datasets from the Silver weather-enriched outputs

**Main inputs**
- Silver weather-enriched datasets

**Main outputs**
- Gold analytical fact datasets
- event checkpoints
- run summaries

**Role in the pipeline**
- shapes the cleaned enriched data into reusable analytical outputs

## `gold_wind_model_inputs`

**Purpose**
- builds the canonical round-level feature dataset used for modeling

**Main inputs**
- Gold weather effect data
- upstream event, result, and weather context

**Main outputs**
- round-level model-input datasets
- event checkpoints
- run summaries

**Role in the pipeline**
- defines the contract between the data platform and the model layer

## `train_round_wind_model`

**Purpose**
- trains the round-level model and publishes versioned training artifacts

**Main inputs**
- Gold model-input datasets

**Main outputs**
- trained model artifacts
- training metadata and manifests
- training run summaries
- updated production fingerprint metadata

**Role in the pipeline**
- refreshes the model used by downstream prediction jobs

## `score_round_wind_model`

**Purpose**
- generates round-level predictions and compares expected versus actual performance

**Main inputs**
- Gold model-input datasets
- trained model artifacts

**Main outputs**
- scored round outputs
- event checkpoints
- run summaries
- run-level error metrics

**Role in the pipeline**
- turns the trained model into usable analytical outputs for reporting and event exploration

## `report_round_weather_impacts`

**Purpose**
- builds the published reporting tables used by the dashboard

**Main inputs**
- scored round outputs
- Gold analytical datasets

**Main outputs**
- published reporting tables in S3
- reporting run summaries

**Role in the pipeline**
- creates the dashboard-facing outputs used for overview, geography, event, and subgroup exploration

## Shared Pipeline Patterns

Several design patterns appear across the jobs.

### Idempotent execution
Jobs are designed to be safe to rerun without duplicating outputs.

### Incremental updates
Most downstream jobs avoid rebuilding everything from scratch when inputs have not changed.

### Event-level checkpoints
Many stages track per-event status in DynamoDB so partial progress is visible and resumable.

### Run summaries
Jobs write run-level summaries with counts, timestamps, and failure information.

### Layered contracts
Each stage consumes outputs from the layer before it rather than reaching back into raw source logic directly.

## Orchestration

The jobs are orchestrated in AWS with:

- **Step Functions**
  - controls job order and shared run context

- **EventBridge**
  - triggers the daily and weekly workflows

- **ECS Fargate**
  - runs the containerized jobs

Each pipeline execution gets a shared context that includes values such as:

- `run_id`
- `pipeline_name`
- `execution_ts`
- `log_level`

That shared context helps tie together logs, checkpoints, run summaries, and monitoring output.
