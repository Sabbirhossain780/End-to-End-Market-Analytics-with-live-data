# End-to-End Market Analytics

Streaming stock analytics with PySpark + a Polygon/Massive.com API, orchestrated
by a set of Apache Airflow pipelines: a hello-world DAG, a 19-task dependency
DAG, and a daily yfinance fetch → walk-forward Ridge regression → error-tracking
pipeline. Originally coursework for CSE 6514: Big Data Analytics.

Full assignment brief: [assignment_brief/CSE6514_Assignment2_Brief.pdf](assignment_brief/CSE6514_Assignment2_Brief.pdf)
Full write-up with screenshots: [report/CSE6514_Assignment2_0424056007.pdf](report/CSE6514_Assignment2_0424056007.pdf)

## Repo layout

```
streaming_analytics/          Part 1: AAPL streaming analytics notebook (PySpark, run on GCP Dataproc)
airflow_pipeline/              Part 2: Airflow DAGs
├── helloworld.py              Task 1 — hello-world smoke test
├── task2_1_dag.py             Task 2.1 — 19-task Bash/Python dependency DAG
├── task2_2_dag.py             Task 2.2 — daily stock fetch/train/predict pipeline
└── scripts/
    ├── hello.sh                helper shell script used by task2_1_dag.py
    ├── counter.py               helper python script used by task2_1_dag.py
    └── task2_2/
        ├── fetch_data.py        pulls full price history for 5 tickers via yfinance
        ├── train_predict.py     walk-forward Ridge regression + relative-error calc
        ├── build_errors_csv.py  assembles the final per-ticker errors table
        ├── diagnose.py          OHLC multicollinearity diagnostic (see note below)
        └── data/                fetched CSVs + the final errors.csv
report/                         Submitted PDF/DOCX write-up (screenshots + explanations)
assignment_brief/               Original assignment handout
screenshots/                    Raw screenshots referenced by the report
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

> **Note:** this repo previously had a real API key committed in the notebook.
> It has been replaced with an environment-variable read. If you're the
> original owner of that key, treat it as compromised and rotate it.

## Notable design decisions (see the report for full detail)

- **Streaming analytics** replays a fixed prior-business-day window instead of
  a live pull, since Polygon's free tier (now branded Massive.com) doesn't
  return true real-time data. The 5-minute cadence and 30-minute total runtime
  are real; only which day's market data gets requested is "replayed."
- **Task 2.2** trains `Ridge` regression (not plain `LinearRegression`) because
  Open/High/Low/Close are almost perfectly collinear on a given day
  (correlation > 0.999, condition number ~1e8–1e9), which made plain OLS
  produce wildly unstable coefficients for 3 of the 5 tickers. `diagnose.py`
  is the script used to confirm this.
- `airflow_pipeline/scripts/hello.sh` is a recreated placeholder — the
  original file used when this ran successfully wasn't among the exported
  materials. Swap in the original if you still have it.
