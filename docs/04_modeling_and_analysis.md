# Modeling and Analysis

## Purpose

This document explains the approach used to estimate how weather affects disc golf scoring.

It focuses on:

- the modeling goal
- the round-level prediction setup
- how weather impact is estimated
- the main evaluation metrics
- the limits of the current approach

## Modeling Goal

The project does not rely on raw score comparisons alone.

Instead, it builds a round-level model that estimates expected performance under the observed conditions for each round, then compares that result to a calm-weather counterfactual. This creates a more useful baseline for asking how much weather changed scoring difficulty.

At a high level, the modeling question is:

**Given the player, event, venue, and weather context for a round, what score would the model expect, and how would that expectation change under calm weather conditions?**

## Current Modeling Approach

The current modeling direction is a **round-level one-stage model**.

That means:

- the model works at the grain of one row per player per round
- the prediction workflow uses a single round-level feature dataset
- the same core feature contract supports both training and prediction

## Target

The model predicts round-level scores.

In practical terms, the target is the player’s observed round result at the round grain used by the model-input dataset.

The prediction is not only used as a forecast. It is used as a reference point for estimating how much weather changed scoring difficulty.

## Main Feature Inputs

The model is trained from the Gold round-level model-input dataset.

The exact fields can evolve, but the main feature groups include:

- **player context**
  - player PDGA rating

- **event and venue context**
  - course/layout context

- **competition context**
  - division

- **weather context**
  - wind-related variables
  - temperature-related variables
  - precipitation-related variables

The model depends on the pipeline to standardize these inputs before training or prediction begins.

## Why a Model Is Used

A simple “bad weather rounds scored worse” comparison is usually not enough because many things change scoring at the same time.

Examples include:

- course difficulty
- layout difficulty
- player ability
- division differences

The model is used to create a more useful expectation for each round rather than treating all rounds as directly comparable without context.

## How Weather Impact Is Estimated

This is the core analytical idea in the project.

For each round, the model is used in two ways:

1. **Observed-weather prediction**
   - predict the round outcome using the actual weather conditions recorded for that round

2. **Calm-weather counterfactual**
   - predict the round outcome again after replacing the weather inputs with a calm-weather reference

The estimated weather impact is then the difference between those two predictions.

In simplified terms:

estimated weather impact = predicted score in observed weather - predicted score in calm weather


This gives the project a way to estimate how much weather changed scoring difficulty while holding the non-weather context of the round constant.

### Why the calm-weather comparison matters

This comparison is important because it avoids treating weather impact as the same thing as total deviation from actual score.

Instead of asking:

- “Was the player better or worse than expected overall?”

the project asks:

- “How much did the expected score change because the weather inputs changed?”

That is a more direct way to estimate the effect of weather itself.

## Training Workflow

The training workflow is handled by `train_round_wind_model`.

At a high level it:

1. loads the round-level Gold model-input dataset
2. computes fingerprints for the source dataset and training request
3. trains the round-level model
4. writes versioned training artifacts and metadata
5. stores training summaries and checkpoint information
6. optionally updates the production training fingerprint used by downstream prediction jobs

This setup makes training runs traceable and repeatable.

## Training Outputs

The training step produces:

- trained model artifacts
- training manifests and metadata
- feature-related metadata
- request fingerprints
- run summaries
- evaluation metrics

A key design choice is that training outputs are versioned and tied to a specific training request fingerprint rather than being treated as anonymous files.

## Prediction Workflow

The prediction workflow is handled by `score_round_wind_model`.

At a high level it:

1. loads the active trained model
2. finds candidate event datasets to score
3. skips events that were already successfully processed unless forced
4. loads each event’s round-level input data
5. generates round-level predictions
6. generates calm-weather counterfactual predictions
7. writes scored round outputs
8. records event-level checkpoints and run summaries
9. refreshes downstream reporting tables through the reporting job

This keeps prediction incremental rather than recomputing every event every time.

## Prediction Outputs

The main prediction output is a scored round dataset.

### Scored rounds
Grain:
- one row per player per round

Typical contents:
- actual round outcome
- predicted round outcome under observed weather
- predicted round outcome under calm weather
- estimated weather impact
- metadata tying the row back to:
  - training fingerprint
  - scoring request fingerprint
  - model artifact version
  - pipeline run

These outputs are later aggregated into the reporting tables used by the dashboard.


## Strengths of the Current Approach

A few things are strong about the current setup:

- **round-level consistency**
  - one main modeling grain keeps the workflow easier to reason about

- **stable data contract**
  - the Gold model-input dataset creates a clear boundary between pipeline logic and model logic

- **counterfactual weather framing**
  - weather impact is estimated by changing weather inputs while holding the rest of the round context fixed

- **versioned artifacts**
  - training outputs are fingerprinted and traceable

- **incremental prediction**
  - downstream prediction work focuses on newly available or changed data

- **integrated monitoring**
  - prediction runs surface `RMSE`, `MAE`, runtime, counts, and cost in the monitoring layer

## Current Limits

The modeling layer is useful, but it should still be read with a few limits in mind:

- the model is only as strong as the upstream event, timing, and weather alignment
- the calm-weather reference is a modeling assumption, not a directly observed outcome
- weather effects are difficult to separate perfectly from every other source of scoring variation
- run-level `RMSE` and `MAE` are useful, but they are not a complete model evaluation framework on their own