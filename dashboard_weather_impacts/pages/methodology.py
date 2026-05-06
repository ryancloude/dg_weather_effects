from __future__ import annotations

import streamlit as st


def _inject_methodology_styles() -> None:
    st.markdown(
        """
        <style>
        .method-page-title {
            font-size: 2.25rem;
            font-weight: 700;
            line-height: 1.02;
            letter-spacing: -0.03em;
            color: #2f3441;
            margin-bottom: 0.3rem;
        }

        .method-page-subtitle {
            font-size: 1rem;
            line-height: 1.62;
            color: #5b6170;
            max-width: 880px;
            margin-bottom: 1.4rem;
        }

        .method-section-kicker {
            font-size: 0.76rem;
            font-weight: 700;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            color: #b45309;
            margin-bottom: 0.3rem;
        }

        .method-section-title {
            font-size: 1.28rem;
            font-weight: 700;
            color: #2f3441;
            margin-bottom: 0.3rem;
        }

        .method-section-body {
            font-size: 0.98rem;
            line-height: 1.6;
            color: #5b6170;
            max-width: 860px;
            margin-bottom: 1rem;
        }

        .method-hero {
            background: linear-gradient(135deg, #fffaf3 0%, #ffffff 100%);
            border: 1px solid #e7dfd2;
            border-radius: 26px;
            padding: 1.4rem 1.5rem;
            margin-bottom: 1.25rem;
            box-shadow: 0 1px 0 rgba(47, 52, 65, 0.04);
        }

        .method-hero-title {
            font-size: 1.52rem;
            font-weight: 700;
            color: #2f3441;
            line-height: 1.15;
            margin-bottom: 0.45rem;
        }

        .method-hero-body {
            font-size: 1rem;
            line-height: 1.68;
            color: #5b6170;
            max-width: 860px;
        }

        .method-grid-2 {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 1rem;
            margin-bottom: 1rem;
        }

        .method-grid-3 {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 1rem;
            margin-bottom: 1rem;
        }

        .method-grid-4 {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 0.9rem;
            margin-bottom: 1rem;
        }

        .method-card {
            background: white;
            border: 1px solid #e7dfd2;
            border-radius: 24px;
            padding: 1.15rem 1.15rem 1rem 1.15rem;
            box-shadow: 0 1px 0 rgba(47, 52, 65, 0.04);
            height: 100%;
            box-sizing: border-box;
        }

        .method-card-tight {
            padding: 0.95rem 1rem 0.9rem 1rem;
        }

        .method-card-title {
            font-size: 1.08rem;
            font-weight: 700;
            color: #2f3441;
            margin-bottom: 0.35rem;
        }

        .method-card-body {
            font-size: 0.97rem;
            line-height: 1.6;
            color: #5b6170;
        }

        .method-step-number {
            width: 2rem;
            height: 2rem;
            border-radius: 999px;
            display: flex;
            align-items: center;
            justify-content: center;
            background: rgba(180, 83, 9, 0.1);
            color: #b45309;
            font-weight: 700;
            margin-bottom: 0.7rem;
        }

        .method-reference-label {
            font-size: 0.74rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #b45309;
            margin-bottom: 0.25rem;
        }

        .method-reference-value {
            font-size: 1.05rem;
            font-weight: 700;
            color: #2f3441;
            margin-bottom: 0.15rem;
        }

        .method-note {
            background: rgba(180, 83, 9, 0.06);
            border: 1px solid rgba(180, 83, 9, 0.14);
            border-radius: 22px;
            padding: 0.95rem 1.05rem;
            margin-bottom: 1.1rem;
        }

        .method-note-title {
            font-size: 0.92rem;
            font-weight: 700;
            color: #8a4b10;
            margin-bottom: 0.2rem;
        }

        .method-note-body {
            font-size: 0.95rem;
            line-height: 1.55;
            color: #5b6170;
        }

        .method-note-body a {
            color: #8a4b10;
            font-weight: 700;
            text-decoration: none;
        }

        .method-note-body a:hover {
            text-decoration: underline;
        }

        @media (max-width: 1100px) {
            .method-grid-4 {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }
        }

        @media (max-width: 800px) {
            .method-grid-2,
            .method-grid-3,
            .method-grid-4 {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_methodology() -> None:
    _inject_methodology_styles()

    st.markdown(
        """
        <div class="method-page-title">Methodology</div>
        <div class="method-page-subtitle">
            This dashboard estimates how weather changes scoring difficulty by comparing model predictions under observed
            round conditions to predictions under a fixed reference baseline.
        </div>

        <div class="method-hero">
            <div class="method-section-kicker">Approach</div>
            <div class="method-hero-title">Estimate the effect of weather in strokes</div>
            <div class="method-hero-body">
                The goal of this project is not just to describe windy rounds. It is to estimate how much wind and other
                weather conditions change expected scoring difficulty after accounting for event, layout, and round-level
                context. The result is a model-based view of weather impact expressed in strokes.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="method-note">
            <div class="method-note-title">Project Repository</div>
            <div class="method-note-body">
                View the full project, pipeline code, and documentation on
                <a href="https://github.com/ryancloude/dg_weather_effects" target="_blank" rel="noopener noreferrer">GitHub</a>.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="method-section-kicker">Data Sources</div>
        <div class="method-grid-2">
            <div class="method-card">
                <div class="method-card-title">PDGA Scoring and Event Context</div>
                <div class="method-card-body">
                    The pipeline ingests PDGA live scoring and event metadata, then normalizes it into typed round-level
                    records. Those records provide event, player, round, course, and layout context for each scored round.
                </div>
            </div>
            <div class="method-card">
                <div class="method-card-title">Weather Observations</div>
                <div class="method-card-body">
                    Weather observations are aligned to event location and round timing. Some rounds include recorded tee
                    times, while others require estimated timing based on event dates and available scoring timestamps. That
                    process creates round-level inputs such as observed wind speed, gust speed, temperature, and
                    precipitation context.
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="method-section-kicker">Process</div>
        <div class="method-section-title">How the estimate is produced</div>
        <div class="method-section-body">
            Each reported weather effect comes from rescoring the same round under observed weather and under a fixed
            reference baseline.
        </div>

        <div class="method-grid-4">
            <div class="method-card">
                <div class="method-step-number">1</div>
                <div class="method-card-title">Collect scored rounds</div>
                <div class="method-card-body">
                    Capture replayable scoring and event records with stable round-level context.
                </div>
            </div>
            <div class="method-card">
                <div class="method-step-number">2</div>
                <div class="method-card-title">Join weather to rounds</div>
                <div class="method-card-body">
                    Align weather using event location and the best round timing information available.
                </div>
            </div>
            <div class="method-card">
                <div class="method-step-number">3</div>
                <div class="method-card-title">Predict expected scoring</div>
                <div class="method-card-body">
                    Estimate expected strokes for the round under observed weather conditions.
                </div>
            </div>
            <div class="method-card">
                <div class="method-step-number">4</div>
                <div class="method-card-title">Compare to reference weather</div>
                <div class="method-card-body">
                    Rescore the round under a fixed baseline and treat the prediction gap as weather impact.
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="method-section-kicker">Reference Conditions</div>
        <div class="method-grid-4">
            <div class="method-card method-card-tight">
                <div class="method-reference-label">Wind Speed</div>
                <div class="method-reference-value">0 mph</div>
                <div class="method-card-body">Removes sustained wind from the round.</div>
            </div>
            <div class="method-card method-card-tight">
                <div class="method-reference-label">Wind Gust</div>
                <div class="method-reference-value">1 mph</div>
                <div class="method-card-body">A near-calm gust reference.</div>
            </div>
            <div class="method-card method-card-tight">
                <div class="method-reference-label">Temperature</div>
                <div class="method-reference-value">80 F</div>
                <div class="method-card-body">A warm neutral temperature reference.</div>
            </div>
            <div class="method-card method-card-tight">
                <div class="method-reference-label">Precipitation</div>
                <div class="method-reference-value">None</div>
                <div class="method-card-body">Rounds are rescored under dry conditions.</div>
            </div>
        </div>

        <div class="method-note">
            <div class="method-note-title">Interpretation</div>
            <div class="method-note-body">
                If a round shows <strong>+1.20 added strokes from wind</strong>, the model expects that round to play about
                1.20 strokes harder under the observed wind than it would under the fixed reference wind baseline.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="method-section-kicker">Metric Definitions</div>
        <div class="method-section-title">What the main dashboard metrics mean</div>
        <div class="method-grid-2">
            <div class="method-card method-card-tight">
                <div class="method-card-title">Added Strokes from Wind</div>
                <div class="method-card-body">
                    The difference between predicted strokes under observed weather and predicted strokes under the
                    reference wind baseline.
                </div>
            </div>
            <div class="method-card method-card-tight">
                <div class="method-card-title">Total Weather Added Strokes</div>
                <div class="method-card-body">
                    The difference between predicted strokes under observed weather and predicted strokes under the full
                    reference weather baseline.
                </div>
            </div>
            <div class="method-card method-card-tight">
                <div class="method-card-title">Observed Wind</div>
                <div class="method-card-body">
                    Average sustained wind speed associated with the round after weather alignment.
                </div>
            </div>
            <div class="method-card method-card-tight">
                <div class="method-card-title">Observed Wind Gust</div>
                <div class="method-card-body">
                    Average gust speed associated with the round after weather alignment.
                </div>
            </div>
            <div class="method-card method-card-tight">
                <div class="method-card-title">Observed Temperature</div>
                <div class="method-card-body">
                    Average round temperature used as part of the weather context.
                </div>
            </div>
            <div class="method-card method-card-tight">
                <div class="method-card-title">Rounds / Events Tracked</div>
                <div class="method-card-body">
                    Coverage metrics showing how many scored rounds and events contribute to each aggregate view.
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="method-section-kicker">Limitations</div>
        <div class="method-section-title">Important context when reading the estimates</div>
        <div class="method-grid-3">
            <div class="method-card method-card-tight">
                <div class="method-card-title">Model-Based Estimate</div>
                <div class="method-card-body">
                    These values are modeled estimates, not direct causal proof. They are designed to approximate how much
                    weather changes expected scoring difficulty.
                </div>
            </div>
            <div class="method-card method-card-tight">
                <div class="method-card-title">Weather Alignment Uncertainty</div>
                <div class="method-card-body">
                    Weather alignment depends on the timing information available for each round. Some rounds include
                    recorded tee times, while others require estimated timing from event dates and scoring activity.
                </div>
            </div>
            <div class="method-card method-card-tight">
                <div class="method-card-title">Context Still Matters</div>
                <div class="method-card-body">
                    Course design, layout difficulty, field strength, and division mix still influence scoring. Weather
                    impact should be interpreted as one important component, not the only one.
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="method-section-kicker">Technical Notes</div>
        <div class="method-section-title">How the pipeline is structured</div>
        <div class="method-section-body">
            The project is built as a production-style data pipeline, but the dashboard keeps the presentation layer focused
            on results first.
        </div>

        <div class="method-grid-3">
            <div class="method-card method-card-tight">
                <div class="method-card-title">Data Layers</div>
                <div class="method-card-body">
                    Raw source snapshots are preserved first, then normalized into typed intermediate datasets, and finally
                    published into analytics-ready reporting tables used by the dashboard.
                </div>
            </div>
            <div class="method-card method-card-tight">
                <div class="method-card-title">Weather Alignment</div>
                <div class="method-card-body">
                    Weather is joined at round level using event location and the best timing information available,
                    combining recorded tee times when present with estimated timing when they are not.
                </div>
            </div>
            <div class="method-card method-card-tight">
                <div class="method-card-title">Modeling Logic</div>
                <div class="method-card-body">
                    The scoring model estimates expected strokes under observed weather and under fixed reference
                    conditions. The difference between those predictions becomes the estimated weather effect in strokes.
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
