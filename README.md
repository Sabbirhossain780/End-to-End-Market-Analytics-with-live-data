# End-to-End Market Analytics

Streaming stock analytics with PySpark on GCP Dataproc, orchestrated end to end
by Apache Airflow — from a live-style ingestion loop, through a 19-task
dependency DAG, to a daily fetch → train → predict → error-tracking pipeline
for five tickers. Originally built as coursework for CSE 6514: Big Data
Analytics; restructured here as a standalone project.

## Overview

This repo has two halves that share one theme — turning raw market data into
something continuously analyzed and orchestrated, not just a one-off script:

1. **Streaming analytics** ([streaming_analytics/](streaming_analytics/)) — a
   PySpark notebook that polls a stock API every 5 minutes, incrementally
   merges each pull into a running Spark DataFrame with an anti-join (so
   overlapping timestamps get refreshed, not duplicated), and maintains a
   rolling 30-minute moving average of open/high/low/close/volume.
2. **Airflow pipelines** ([airflow_pipeline/](airflow_pipeline/)) — three DAGs
   of increasing complexity:
   - `helloworld.py` — a one-task smoke test for the Airflow install.
   - `task2_1_dag.py` — a 19-task DAG (11 Bash + 8 Python operators) exercising
     fan-out/fan-in dependencies, manual triggers, and scheduled runs.
   - `task2_2_dag.py` — a daily pipeline that pulls AAPL/GOOGL/META/MSFT/AMZN
     price history via `yfinance`, retrains a Ridge regression model per
     ticker in a walk-forward loop, predicts next-day highs, and writes a
     relative-error table — with results passed between tasks over XCom.

## Pipeline at a glance

```
Streaming (PySpark, every 5 min for 30 min)
  Massive/Polygon API ──▶ anti-join merge ──▶ main_df (raw ticks)
                                          └──▶ 30-min moving average ──▶ ma_df

Airflow: task2_2_dag (daily)
  fetch_data ──▶ train_predict_AAPL   ─┐
              ├─▶ train_predict_GOOGL ─┤
              ├─▶ train_predict_META  ─┼─▶ build_errors_csv ──▶ errors.csv
              ├─▶ train_predict_MSFT  ─┤
              └─▶ train_predict_AMZN  ─┘
```

## Results

**Streaming run** — 7 pulls, 5 minutes apart, 91 one-minute AAPL bars merged
with no duplicates, and one 30-minute moving-average row per pull:

| UTC Timestamp | c | l | h | o | v |
|---|---|---|---|---|---|
| 2026-08-10 13:00 | 309.18 | 309.05 | 309.1986 | 309.05 | 11103.03 |
| 2026-08-10 14:30 | 307.10 | 306.90 | 307.19 | 306.90 | 93995.84 |

**Forecast errors** (`airflow_pipeline/scripts/task2_2/data/errors.csv`) —
relative error of next-day-high predictions, walk-forward over the last 5
trading days, one column per company:

| Date | APPLE | GOOGLE | META | MICROSOFT | AMAZON |
|---|---|---|---|---|---|
| 8/7/2026  | 0.0032 | 0.0093 | -0.0031 | 0.0006 | -0.0103 |
| 8/13/2026 | -0.0027 | -0.0023 | -0.0135 | -0.0068 | 0.0041 |

More detail, plus every intermediate screenshot, is in the full write-up:
[report/CSE6514_Assignment2_0424056007.pdf](report/CSE6514_Assignment2_0424056007.pdf).
Original assignment brief: [assignment_brief/CSE6514_Assignment2_Brief.pdf](assignment_brief/CSE6514_Assignment2_Brief.pdf).

## Tech stack

PySpark · Apache Airflow 2.10 (SequentialExecutor) · pandas · scikit-learn
(Ridge regression) · yfinance · Massive/Polygon REST API · GCP Dataproc

## Repo layout

```
streaming_analytics/            Part 1: AAPL streaming analytics notebook (PySpark, run on GCP Dataproc)
airflow_pipeline/                Part 2: Airflow DAGs
├── helloworld.py                 Task 1 — hello-world smoke test
├── task2_1_dag.py                Task 2.1 — 19-task Bash/Python dependency DAG
├── task2_2_dag.py                Task 2.2 — daily stock fetch/train/predict pipeline
└── scripts/
    ├── hello.sh                   helper shell script used by task2_1_dag.py
    ├── counter.py                 helper python script used by task2_1_dag.py
    └── task2_2/
        ├── fetch_data.py          pulls full price history for 5 tickers via yfinance
        ├── train_predict.py       walk-forward Ridge regression + relative-error calc
        ├── build_errors_csv.py    assembles the final per-ticker errors table
        ├── diagnose.py            OHLC multicollinearity diagnostic (see notes below)
        └── data/                  fetched CSVs + the final errors.csv
report/                          Submitted PDF/DOCX write-up (screenshots + explanations)
assignment_brief/                Original assignment handout
screenshots/                     Raw screenshots referenced by the report
```

## Setup

```bash
python -m venv venv
source venv/bin/activate   # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

The streaming notebook reads its API key from an environment variable — set it
before launching Jupyter:

```bash
export POLYGON_API_KEY="your-key-here"
```

To run the Airflow DAGs, point `AIRFLOW_HOME` at a directory containing an
`airflow.cfg` with `executor = SequentialExecutor`, symlink or copy
`airflow_pipeline/` into `$AIRFLOW_HOME/dags`, then:

```bash
airflow webserver -p 8080   # in one terminal
airflow scheduler           # in another
```

## Notable design decisions

- **Streaming analytics replays a fixed prior-business-day window** instead of
  a live pull, since the stock API's free tier (Polygon.io, now rebranded
  Massive.com) doesn't return true real-time data. The 5-minute cadence and
  30-minute total runtime are real; only which day's market data gets
  requested is "replayed."
- **Task 2.2 trains `Ridge` regression, not plain `LinearRegression`**,
  because Open/High/Low/Close are almost perfectly collinear on any given day
  (correlation > 0.999, feature-matrix condition number ~1e8–1e9), which made
  plain OLS produce wildly unstable coefficients for 3 of the 5 tickers.
  `diagnose.py` is the script used to confirm this before switching.
- `airflow_pipeline/scripts/hello.sh` is a recreated placeholder; the
  original wasn't preserved.

## Author

Sabbir Hossain Muni — MSc. in Data Science, Department of CSE, BUET
