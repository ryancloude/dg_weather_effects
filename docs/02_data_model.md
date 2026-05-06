# Data Model

## Purpose

This document describes the main datasets used across the project, their grain, and how they relate to one another.

It focuses on the datasets that matter most for the pipeline, model, and dashboard rather than listing every field in every table.

## Overview

The project uses four connected layers:

1. **Bronze**
   - raw source snapshots and source-state metadata

2. **Silver**
   - cleaned and standardized competition and weather datasets

3. **Gold**
   - analysis-ready datasets and model-input features

4. **Modeling / Reporting**
   - prediction outputs and dashboard reporting tables

```text
Raw source snapshots
        ↓
Cleaned and standardized datasets
        ↓
Analysis-ready datasets and model inputs
        ↓
Predictions, reporting tables, and dashboard outputs
```

## Core Entities

The main business entities are:

- event
- division
- round
- player round
- player hole
- weather observation

Weather observations are aligned to event timing and location, then joined back onto round- and hole-level competition data.

## Bronze Layer

The Bronze layer stores replayable raw source data.

### Main Bronze domains

- **PDGA event metadata**
  - raw event HTML in S3
  - parsed event metadata in DynamoDB

- **PDGA live results**
  - raw live-results JSON in S3
  - fetch state in DynamoDB

- **Historical weather observations**
  - raw weather JSON in S3
  - weather fetch state in DynamoDB

## DynamoDB Metadata Model

DynamoDB stores the project’s state and operational metadata.

### Important item types

- event metadata
- live results state
- weather observation state
- geocode cache
- event-level checkpoints
- run summaries

### Important key patterns

- `EVENT#<event_id> / METADATA`
  - one event metadata record

- `EVENT#<event_id> / LIVE_RESULTS#DIV#<division>#ROUND#<round_number>`
  - one live-results fetch state record

- `EVENT#<event_id> / WEATHER_OBS#ROUND#<round_number>#PROV#<provider>#SRC#<source_id>`
  - one weather fetch state record

- `PIPELINE#<pipeline_name> / EVENT#<event_id>`
  - one downstream pipeline checkpoint for an event

- `RUN#<run_id> / <PIPELINE>#SUMMARY`
  - one run summary record

## Silver Layer

The Silver layer holds cleaned and standardized datasets.

### Silver live results

#### `player_rounds`
Grain:
- one row per player per round

Role:
- main round-level competition dataset
- key join point for weather alignment and modeling

#### `player_holes`
Grain:
- one row per player per hole

Role:
- detailed hole-level competition dataset
- supports finer-grained analysis

### Silver weather observations

#### weather observations
Grain:
- one row per observation timestamp per event/round context

Role:
- cleaned historical weather dataset used for downstream joins

### Silver weather enriched

#### weather-enriched player rounds
Grain:
- one row per player per round

Role:
- round results with attached weather context

#### weather-enriched player holes
Grain:
- one row per player per hole

Role:
- hole results with attached weather context

## Gold Layer

The Gold layer contains analysis-ready outputs.

### Gold wind effects

Role:
- reusable analytical datasets for studying how weather relates to scoring outcomes

### Gold wind model inputs

#### round model inputs
Grain:
- one row per player per round

Role:
- canonical model-input dataset for training and prediction

## Modeling Outputs

### Training outputs

Role:
- versioned model artifacts and training metadata

Examples:
- trained model files
- training manifests
- request fingerprints

### Prediction outputs

#### scored rounds
Grain:
- one row per player per round

Role:
- predicted versus actual round outcomes used for downstream reporting and analysis

## Reporting Layer

The reporting layer publishes the outputs used directly by the dashboard.

### Main reporting tables

- **Overview**
  - `weather_overview`
  - `weather_impact_distribution`
  - `weather_wind_impact_points`

- **Geography**
  - `weather_by_state`

- **Event Explorer**
  - `weather_by_event`
  - `weather_by_event_round`

- **Subgroup views**
  - `weather_by_division`
  - `weather_by_rating_band`
  - `weather_by_course_layout`
  - `weather_by_wind_bucket`
  - `weather_by_temperature_band`

## Grain Summary

The main dataset grains are:

- **event-level**
  - event metadata
  - some reporting outputs

- **event / division / round**
  - live results state
  - weather fetch state

- **player / round**
  - Silver round results
  - weather-enriched rounds
  - Gold model inputs
  - prediction outputs

- **player / hole**
  - Silver hole results
  - weather-enriched holes

- **run-level**
  - run summaries
  - monitoring outputs

## Lineage Overview

```text
PDGA event pages
    → event metadata

PDGA live results
    → Silver round results
    → Silver hole results

Historical weather responses
    → Silver weather observations

Silver round + hole results
Silver weather observations
    → Silver weather enriched

Silver weather enriched
    → Gold wind effects
    → Gold model inputs

Gold model inputs
    → trained model artifacts
    → prediction outputs

Prediction outputs + reporting logic
    → published dashboard tables
```
