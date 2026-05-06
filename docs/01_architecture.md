# Architecture

## Purpose

This project is built as an end-to-end data platform for measuring how weather affects disc golf scoring.

At a high level, the system does four things:

1. collects tournament and weather data from external sources
2. stores raw source snapshots so runs can be audited and replayed
3. transforms that data into cleaned and analysis-ready datasets
4. publishes model-based results for reporting, monitoring, and dashboard exploration

The architecture is designed to separate ingestion, transformation, modeling, and presentation so each part of the system can evolve without tightly coupling everything together.

## System Overview

The project combines two main source domains:

- **PDGA competition data**
  - event metadata
  - live results
  - round and hole outcomes

- **Historical weather data**
  - weather observations aligned to event location and timing

Those sources move through a layered pipeline:

```text
PDGA event data        PDGA live results        Historical weather
       |                      |                        |
       +-----------+----------+------------------------+
                   |
                   v
            Bronze raw data in S3
                   |
                   v
        Silver cleaned / normalized datasets
                   |
                   v
      Gold analysis-ready datasets and features
                   |
          +--------+---------+
          |                  |
          v                  v
      Model training   Predictions + reporting tables
                                |
                                v
                       Streamlit dashboard
```

The runtime and control plane around that data flow is AWS-native:

```text
EventBridge schedules
        |
        v
  Step Functions
        |
        v
 ECS Fargate jobs
        |
        +--> S3 data layers
        +--> DynamoDB checkpoints and run summaries
        +--> Athena reporting refreshes

After pipeline completion:
Step Functions status event
        |
        v
 EventBridge
        |
        v
 Monitoring Lambda
        |
        v
 SES email summary
```

## Design Principles

A few design choices shape the system:

### Layered data flow

The project uses Bronze, Silver, and Gold layers so raw inputs, cleaned datasets, and analysis-ready outputs are clearly separated.

### Replayability

Raw source snapshots are stored in S3 rather than discarded after parsing. That makes it possible to:
- audit source behavior
- debug parser issues
- rerun downstream logic without refetching external data

### Idempotent processing

Jobs are designed to be rerun safely. This matters for:
- scheduled updates
- backfills
- recovery after partial failures

### Stateful orchestration

The system does not rely only on logs to understand what happened. DynamoDB stores:
- metadata
- per-event checkpoints
- run summaries
- pipeline-run records

### Separation of computation and presentation

The dashboard does not compute the pipeline outputs itself. It reads published reporting datasets from S3. That keeps the presentation layer thinner and makes the analytical outputs reusable outside the app.

## Main Architecture Components

## AWS Services and Their Roles

### S3

S3 is the main storage layer for the project. It stores:

- Bronze raw source snapshots
- Silver cleaned parquet datasets
- Gold analysis-ready datasets
- model artifacts
- prediction outputs
- published reporting tables
- Athena query results

S3 is the backbone of the data platform because it holds both the replayable raw inputs and the durable analytical outputs.

### DynamoDB

DynamoDB is used for pipeline state and metadata.

It stores:

- event metadata
- live results fetch state
- weather fetch state
- geocode cache entries
- event-level processing checkpoints
- run summaries
- pipeline-run status records

DynamoDB gives the project a persistent operational memory, which is especially important for incremental updates and failure recovery.

### ECS Fargate

Each ingestion, transform, modeling, and reporting job runs as a containerized ECS Fargate task.

This includes jobs such as:

- `ingest_pdga_event_pages`
- `ingest_pdga_live_results`
- `silver_pdga_live_results`
- `silver_weather_enriched`
- `gold_wind_model_inputs`
- `train_round_wind_model`
- `score_round_wind_model`
- `report_round_weather_impacts`

Using Fargate keeps execution isolated by job and makes it easier to allocate job-specific CPU, memory, and timeout settings.

### Step Functions

Step Functions orchestrates the pipelines.

It is responsible for:

- creating a shared `run_id`
- defining job order
- passing common runtime context to every ECS task
- marking pipeline runs as `RUNNING`, `SUCCEEDED`, or `FAILED`
- separating the daily pipeline from the weekly model refresh pipeline

This is the control layer that turns the individual jobs into coherent workflows.

### EventBridge

EventBridge is used in two ways:

- to schedule the pipelines
- to trigger post-run monitoring after Step Functions execution completes

In practice, EventBridge is the project’s scheduler and event router.

### Athena

Athena supports the reporting layer.

It is used for:

- reporting-table refresh workflows
- query-based published outputs used by the dashboard
- query-cost tracking in pipeline monitoring

Athena is not the main transformation engine for the entire platform, but it plays an important role in the reporting and presentation layer.

### Lambda

Lambda is used for post-run monitoring.

After a pipeline finishes, a Lambda function collects:

- pipeline timing from Step Functions
- run summaries from DynamoDB
- container metrics from CloudWatch
- estimated cost information

It then formats the results into a pipeline summary.

### SES

SES is used to email the monitoring summaries after pipeline runs complete.

This makes the operational layer easier to review without manually opening logs and metrics every time.

### ECR

ECR stores the container images for the pipeline jobs.

Each job has its own image repository so the execution layer stays containerized and deployable through AWS rather than being tied to one local runtime.

### SSM Parameter Store

SSM Parameter Store is used for shared configuration values, including values needed by pipeline jobs and the current production training fingerprint.

## Pipeline Structure

The project currently has two orchestrated workflows.

## Daily Pipeline

The daily pipeline is responsible for refreshing the data and republishing downstream outputs. It:

- pulls newly available source data
- applies incremental updates to downstream layers
- generates model predictions for new data
- republishes reporting tables for the dashboard

The current daily pipeline job order is:

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

The weekly pipeline retrains the round-level model and then republishes downstream outputs that depend on that refreshed model.

The current weekly job order is:

1. `train_round_wind_model`
2. `score_round_wind_model`
3. `report_round_weather_impacts`

This separation keeps model retraining distinct from the daily data refresh workflow.

## Data Layers

## Bronze Layer

The Bronze layer is the raw landing zone for source data.

Examples include:

- PDGA event HTML
- PDGA live results JSON
- Open-Meteo weather responses

The main purpose of Bronze is preservation. It keeps the original source responses available for later replay, inspection, and debugging.

## Silver Layer

The Silver layer converts raw source inputs into cleaned and standardized datasets.

Examples include:

- round-level results
- hole-level results
- hourly weather observations
- weather-enriched round and hole outputs

This is the part of the architecture where parsing, normalization, typing, and data-quality checks become most important.

## Gold Layer

The Gold layer contains analysis-ready datasets and feature tables.

Examples include:

- weather effect datasets
- round-level model inputs
- published reporting datasets for the dashboard

Gold is the layer intended for reuse by modeling, reporting, and presentation consumers.

## Modeling and Reporting Layer

This layer turns prepared data into model outputs and dashboard-ready artifacts.

It includes:

- model training artifacts
- event-level prediction outputs
- scored round parquet outputs
- published reporting tables

This is where the system moves from data preparation into analysis delivery.

## Runtime Context and Job Execution

A key part of the architecture is that each pipeline run gets a shared execution context.

Step Functions creates values such as:

- `run_id`
- `pipeline_name`
- `app_env`
- `execution_ts`
- `log_level`

Those values are passed into ECS tasks as environment variables. That makes it possible to connect:

- logs
- DynamoDB run summaries
- pipeline execution metadata
- monitoring outputs

back to the same pipeline run.

This shared context is especially important for observability and debugging.

## Metadata and Checkpointing

The main metadata table in DynamoDB is used across Bronze, Silver, Gold, and monitoring layers.

The table includes several important item types:

- event metadata items
- live results state items
- weather state items
- geocode cache items
- event-level processing checkpoints
- run summary items
- pipeline-run items

In practical terms, this allows the system to answer questions like:

- Which events have already been processed?
- Which events changed since the last run?
- Which pipeline step failed for a given event?
- How many rows or events were written in a run?
- What happened in a specific `run_id`?

This metadata layer is a major reason the project can support incremental processing instead of acting like a stateless batch script.

## Reporting and Dashboard Architecture

The dashboard does not query operational source systems directly.

Instead, it reads published reporting datasets from S3. The dashboard layer is intentionally downstream of the pipeline and uses a read-only access pattern over prepared outputs.

Today the Streamlit app reads published tables such as:

- `weather_overview`
- `weather_impact_distribution`
- `weather_wind_impact_points`
- `weather_by_state`
- `weather_by_event`
- `weather_by_event_round`
- subgroup tables such as division, rating-band, course-layout, wind-bucket, and temperature-band views

This matters architecturally because it keeps dashboard behavior stable and makes the reporting outputs reusable for other consumers later.

## Monitoring Architecture

Monitoring runs after pipeline completion rather than inside the middle of the pipeline.

That architecture choice matters because the monitoring summary should reflect the true final pipeline status.

The monitoring flow is:

1. a Step Functions execution finishes
2. EventBridge receives the execution status change
3. the monitoring Lambda is invoked
4. the Lambda reads:
   - Step Functions execution history
   - DynamoDB run summaries
   - CloudWatch container metrics
   - Athena usage signals where available
5. it renders a summary email
6. SES sends the email

The current monitoring summary includes:

- job-level runtime
- total pipeline runtime
- processed and failed counts
- model error metrics for prediction runs
- average and peak CPU utilization
- average and peak memory utilization
- estimated compute and query cost

This makes the project easier to operate and easier to evaluate over time.

## Why the Architecture Is Set Up This Way

This architecture is not trying to be the most minimal possible implementation. It is trying to be a clean, understandable system that can answer a real analytical question while showing good engineering habits.

A few of the important tradeoffs are:

- raw data is stored instead of discarded, which adds storage cost but improves replayability
- orchestration is explicit instead of hidden in one script, which adds structure but improves visibility
- metadata is stored centrally in DynamoDB, which adds complexity but makes incremental processing much more practical
- dashboard outputs are pre-published instead of computed live, which reduces flexibility in the app but improves reliability and performance

## Current Boundaries of the System

The architecture is strong for:

- scheduled ingestion and refresh
- reproducible layered transformations
- model retraining and downstream refresh
- event-level and aggregate reporting
- dashboard delivery
- run monitoring

The parts that still leave room to grow are mostly around:

- richer result storytelling
- broader published findings
- deeper benchmarking and model comparison
- more polished architecture visuals
- additional long-term historical monitoring and trend comparison

## Related Docs

This architecture document is the system-level view of the project. The deeper details live in the related docs:

- `docs/02_data_model.md`
- `docs/03_pipeline_jobs.md`
- `docs/04_modeling_and_analysis.md`
- `docs/05_monitoring_and_operations.md`
- `docs/06_setup_and_reproducibility.md`
- `docs/07_results_and_findings.md`
- `docs/08_design_decisions_and_tradeoffs.md`
- `docs/appendix_code_walkthrough.md`
```