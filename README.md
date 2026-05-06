# Disc Golf Weather Effects

How much does weather change disc golf scoring? This project combines PDGA tournament data with historical weather observations to estimate how conditions like wind, temperature, and precipitation affect player performance. I built an AWS-based pipeline and dashboard that turns raw event and weather data into analysis-ready datasets, model-based expectations, and interactive reporting.

## Live Dashboard

The public-facing part of the project is an interactive Streamlit dashboard:

[discgolfweathereffects.streamlit.app](https://discgolfweathereffects.streamlit.app/)

The dashboard is designed to make the project easier to explore without reading the code or pipeline outputs directly. It brings together the published reporting tables into a few focused views:

- **Overview**: high-level patterns in overall weather impact
- **Geography**: where weather effects appear strongest across locations
- **Event Explorer**: event-level breakdowns and deeper inspection
- **Methodology**: how the estimates are built and how to interpret them

## Architecture

### System Flow

The project starts by collecting tournament metadata, round results, and historical weather data. Raw source snapshots are stored first, then transformed into cleaned datasets, then into analysis-ready datasets used for modeling, reporting, and the dashboard.

```text
PDGA event data + weather data
            ↓
      Raw source snapshots
            ↓
   Cleaned / standardized data
            ↓
   Analysis-ready datasets
            ↓
 Model predictions + reporting tables
            ↓
      Streamlit dashboard
```

### AWS Services Used

- **S3** stores raw source data, cleaned datasets, model artifacts, and dashboard/reporting outputs
- **DynamoDB** tracks checkpoints, metadata, and run summaries
- **ECS Fargate** runs the ingestion, transformation, modeling, and reporting jobs
- **Step Functions** orchestrates the pipeline jobs
- **EventBridge** triggers scheduled workflows and post-run monitoring
- **Athena** supports reporting-table refreshes and analytical querying
- **Lambda** powers post-run monitoring and summary generation
- **SES** sends automated monitoring emails after pipeline runs

### Technical Highlights

- **Replayable raw data**  
  Raw source snapshots are stored in S3 so the pipeline can be audited and rerun without relying only on current source behavior.

- **Idempotent pipeline design**  
  Jobs are built to be safe to rerun, which matters for both scheduled processing and backfills.

- **Checkpointed processing**  
  DynamoDB is used to track job state, run summaries, and event-level progress.

- **Layered data model**  
  Bronze, Silver, and Gold layers separate raw ingestion from cleaned outputs and analysis-ready datasets.

- **Model-backed analysis**  
  The project does not rely only on raw score comparisons. It uses a round-level model to estimate expected outcomes and study how weather shifts those expectations.

- **Operational monitoring**  
  Pipeline runs are summarized automatically with timing, event counts, resource usage, and estimated cost.

- **Public-facing delivery**  
  The analytical outputs are surfaced through a deployed dashboard rather than staying buried in notebooks.


## Data Pipeline

The project is organized into three main data layers.

### Bronze

The Bronze layer stores replayable raw source snapshots. This is the first landing point for external data and preserves the original source responses for auditing, debugging, and reprocessing.

Examples include:

- PDGA event pages
- PDGA live-results payloads
- historical weather responses

### Silver

The Silver layer contains cleaned and standardized datasets. This is where raw source data is parsed, normalized, typed, and aligned into stable analytical structures.

Examples include:

- round-level results
- hole-level results
- hourly weather observations
- weather-enriched event data

### Gold

The Gold layer contains analysis-ready datasets used for reporting and modeling. These outputs are designed to be easier to query, compare, and reuse across the project.

Examples include:

- round-level model inputs and round-level model outputs
- published dashboard tables

### Modeling and Reporting

The final layer turns the prepared data into model predictions, reporting tables, and dashboard outputs. This is where the project moves from data preparation into analysis delivery.

## Pipeline Jobs

The project is split into two scheduled workflows.

### Daily Pipeline

This pipeline pulls new data from the source systems, applies incremental updates to the downstream data layers, generates model predictions for the newly available data, and republishes the reporting tables used by the dashboard.

- `ingest_pdga_event_pages`
- `ingest_pdga_live_results`
- `silver_pdga_live_results`
- `ingest_weather_observations`
- `silver_weather_observations`
- `silver_weather_enriched`
- `gold_wind_effects`
- `gold_wind_model_inputs`
- `score_round_wind_model`
- `report_round_weather_impacts`

### Weekly Model Refresh Pipeline

This pipeline retrains the round-level model and republishes the downstream prediction and reporting outputs.

- `train_round_wind_model`
- `score_round_wind_model`
- `report_round_weather_impacts`

Both pipelines are orchestrated in AWS with Step Functions and EventBridge.

## Monitoring

Both pipelines include automated post-run monitoring.

Each run produces an email summary that includes:

- how long each job took
- total pipeline runtime
- processed and failed event counts
- model error metrics for prediction jobs
- average and peak CPU and memory utilization
- estimated cost by job and for the full pipeline

The goal is to make pipeline runs easier to understand at a glance and easier to debug when something goes wrong.


## Running the Project

The pipelines are meant to run in AWS, but each job can also be built and tested locally with Docker.

### Run a job locally with Docker

Each pipeline job has its own Dockerfile and entrypoint script. In practice, that means you can build an image for a job and then run it by passing the normal CLI arguments directly to the container.

Example: build and run the event-page ingestion job locally

```
docker build -f .\docker\dockerfiles\Dockerfile.event_pages -t dgwe/ingest-pdga-event-pages .
docker run --rm dgwe/ingest-pdga-event-pages --incremental
```

Example: build and run the model training job locally

```
docker build -f .\docker\dockerfiles\Dockerfile.train_round_wind_model -t dgwe/train-round-wind-model .
docker run --rm dgwe/train-round-wind-model --log-level INFO
```

Example: build and run the prediction job locally

```
docker build -f .\docker\dockerfiles\Dockerfile.score_round_wind_model -t dgwe/score-round-wind-model .
docker run --rm dgwe/score-round-wind-model --log-level INFO
```

Example: build and run the reporting job locally

```
docker build -f .\docker\dockerfiles\Dockerfile.report_round_weather_impacts -t dgwe/report-round-weather-impacts .
docker run --rm dgwe/report-round-weather-impacts --log-level INFO
```

### Deploy to AWS

Build and publish the pipeline images to ECR:

```
powershell -File .\infra_cdk\scripts\publish_pipeline_images.ps1 -ImageTag latest
```

Deploy the shared infrastructure and orchestration stacks:

```
cd .\infra_cdk
cdk deploy dgwe-dev-Shared dgwe-dev-Orchestration
```

The dashboard is deployed separately through Streamlit Community Cloud and reads the published reporting tables from S3.

## How the Repo Is Organized

Instead of splitting the project into a single monolithic pipeline, the repo is organized by stage and responsibility.

- `ingest_*`
  - source collection jobs for PDGA and weather data

- `silver_*`
  - cleaning, standardization, and enrichment jobs

- `gold_*`
  - analysis-ready datasets and model-input preparation

- `train_round_wind_model`
  - model training workflow

- `score_round_wind_model`
  - prediction workflow used to compare expected versus actual round outcomes

- `report_round_weather_impacts`
  - reporting-table generation for the dashboard

- `dashboard_weather_impacts`
  - Streamlit application

- `infra_cdk`
  - AWS infrastructure definitions

- `tests`
  - automated validation for pipeline components

- `wind_impact_analysis`
  - Anallysis of weather effects and modeling expirements

- `docs`
  - architecture, data model, pipeline, modeling, monitoring, and results documentation

## Documentation

The repository documentation is organized to make the project easier to understand from both a technical and analytical perspective.

It includes:

- `docs/01_architecture.md`
- `docs/02_data_model.md`
- `docs/03_pipeline_jobs.md`
- `docs/04_modeling_and_analysis.md`
- `docs/05_monitoring_and_operations.md`
- `docs/06_setup_and_reproducibility.md`
- `docs/07_results_and_findings.md`
- `docs/08_design_decisions_and_tradeoffs.md`
- `docs/appendix_code_walkthrough.md`
