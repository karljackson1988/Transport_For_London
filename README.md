# Transport_For_London

# Transport for London – Service Status & Arrivals Analytics
# Overview

This repository implements an end-to-end analytics pipeline using the Transport for London (TfL) Unified API.
It captures operational service status and train arrivals data on a scheduled basis, persists historical snapshots in GitHub, and exposes the data to Power BI for both operational and analytical reporting.

The design deliberately separates:

episodic operational data (line status),

high-frequency predictive data (arrivals),

visual analytics and insight generation (Power BI).

This mirrors real-world transport analytics and operations monitoring patterns.

# Architecture Summary

TfL Unified API
      →
GitHub Actions (ETL, Python)
      →
Parquet snapshots in GitHub
      →
Power BI Dataflows
      →
Power BI Reports (Live + Historical)


# Stage 1 – ETL via GitHub Actions
# Why GitHub Actions?

Fully serverless for POC

Deterministic scheduling

Versioned historical data

Simple, auditable ETL pipeline

# Data Collected
| Dataset         | Purpose                                      | Frequency        |
| --------------- | -------------------------------------------- | ---------------- |
| **Line Status** | Operational state of each line               | Every 30 minutes |
| **Arrivals**    | Vehicle-level predicted arrivals at stations | Every 15 minutes |


Workflow: Line Status Snapshot (30 minutes)

Schedule: */30 * * * * (UTC)

Endpoint(s):

GET /Line/Mode/{modes}

GET /Line/{ids}/Status

Output:
data/snapshots/tfl_status_YYYY-MM-DD_HHMMSSZ.parquet

Key design choices:

Batched requests to avoid URL length limits

One file per snapshot (append-only)

Preserves concurrent statuses per line (TfL can report multiple at once)


Workflow: Arrivals Snapshot (15 minutes)

Schedule: */15 * * * * (UTC, offset from status)

Endpoint(s):

GET /Line/{id}/Arrivals

Output:
data/arrivals/tfl_arrivals_YYYY-MM-DD_HHMMSSZ.parquet

Key design choices:

Line-level arrivals (covers all stations for that line)

Retry + exponential backoff for 429 protection

Light jitter between requests

Defensive de-duplication (API occasionally repeats predictions)

This produces a vehicle × station × time dataset suitable for:

headway analysis,

bunching detection,

delay propagation hypotheses.



# Stage 2 – Power BI Dataflows

Power BI Dataflows ingest the parquet snapshots directly from GitHub using the GitHub Contents API.

# Service Status Dataflow

Lists files under data/snapshots/

Downloads and combines all parquet files

Adds time-of-day extraction from snapshot_utc

# Arrivals Dataflow

Lists files under data/arrivals/

Combines arrival snapshots into a single fact table

Why Dataflows?

Centralised transformation logic

Reusable semantic layer

Clean separation between ingestion and reporting

# Stage 3 – Power BI Reporting
Key Analytical Concepts

Latest status per line is derived using valid_from_utc, not refresh time.

When multiple concurrent statuses exist, the worst severity wins.

Visuals distinguish current operational state from historical patterns.

# Core DAX Measures

Latest Valid From
```
M_MAX_VALID_FROM =
CALCULATE (
    MAX ( 'Service Snapshots'[valid_from_utc] )
)
```

Latest Status per Line
```
M_LATEST_STATUS =
VAR LineName = SELECTEDVALUE ( 'Service Snapshots'[line_name] )
RETURN
CALCULATE (
    MAX ( 'Service Snapshots'[statusSeverityDescription] ),
    TOPN (
        1,
        FILTER ( ALL ( 'Service Snapshots' ), 'Service Snapshots'[line_name] = LineName ),
        'Service Snapshots'[valid_from_utc], DESC,
        'Service Snapshots'[statusSeverity], ASC
    )
)

```

Count of Lines by Latest Status
```
M_COUNT =
CALCULATE (
    DISTINCTCOUNT ( 'Service Snapshots'[line_name] ),
    FILTER (
        'Service Snapshots',
        'Service Snapshots'[valid_from_utc] = [M_MAX_VALID_FROM]
            && 'Service Snapshots'[statusSeverityDescription] = [M_LATEST_STATUS]
    )
)

```

# Visual Design

The report focuses on operational clarity, not dense tabulation.

Key visuals include:

Donut chart: Number of lines by latest service status

Line health table: Latest status and duration per line

Timeline heatmap: Severity changes across the day

Status key: Explicit mapping of severity codes to descriptions

The report supports:

rapid operational assessment,

prioritisation of issues,

short-term trend identification.


# Design Trade-offs & Rationale

| Decision                     | Rationale                                             |
| ---------------------------- | ----------------------------------------------------- |
| 30-minute status snapshots   | Disruptions are episodic, not second-by-second        |
| 15-minute arrivals snapshots | Preserves headway signal without overwhelming storage |
| Append-only parquet          | Simple, auditable, scalable                           |
| “Worst status wins”          | Operationally defensible and consistent               |
| GitHub as storage            | Transparent, versioned, interview-friendly            |


# Potential Extensions (Out of Scope)

Station-level dwell analysis

Headway regularity scoring

Correlation between arrivals irregularity and line disruptions

Capacity-weighted station impact

These are intentionally left as analytical opportunities, not assumptions.

# Summary

This project demonstrates:

end-to-end data engineering,

real-world API handling,

operational analytics modelling,

and decision-focused visual design.

It prioritises clarity, robustness, and insight over raw data volume.

# Review changes / Enhancements

There are too many .parquet files being loaded into GitHub and eventually being loaded into PowerBi on a very regulr reglar refresh schedule to show close to real time metrics.

Moving forwards it would be better to append hstoric data into a single or even daily file and have the latest snapshot as it's own file using direct query to provide those close to real time analytics.
