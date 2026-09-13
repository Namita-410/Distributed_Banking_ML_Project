# Distributed Machine Learning on Banking Data

**Hadoop • Hive • Apache Spark • Spark ML • Spark Streaming**

A single Colab notebook that walks through a full "big data" pipeline on a bank marketing dataset — from warehouse-style storage and querying, through EDA and predictive modeling, to real-time stream processing and data-parallelism optimizations.

## Overview

The project runs **PySpark in local (single-node) mode inside Google Colab**, which simulates a distributed cluster. Colab doesn't provide a real multi-node Hadoop cluster, so the standard, accepted workaround is used: Spark local mode with Hive support enabled (`enableHiveSupport()`). This gives access to Spark's built-in Hive metastore and the same HiveQL engine a real Hadoop/Hive cluster would use — just running on one machine instead of many.

## Tech Stack

| Part | Objective | Tool |
|---|---|---|
| 1 | Data Storage & Querying | Spark SQL with Hive support (Hive-on-Hadoop style) |
| 2 | Exploratory Data Analysis | Spark DataFrame API |
| 3 | Predictive Modeling | Spark ML (Pipelines, Logistic Regression, Random Forest) |
| 4 | Real-Time Transaction Analysis | Spark Structured Streaming |
| 5 | Data Parallelism | Partitioning, caching, broadcast joins |

## Requirements

- Google Colab (or any environment that can run PySpark)
- `pyspark==3.5.1` (installed in the notebook's setup cell)
- `bank.csv` — the raw dataset, uploaded to the Colab file browser or mounted via Google Drive
- For Part 4: a `streaming_chunks/` folder of 10 pre-split batch CSVs, or regenerate them from `bank.csv` using the helper cell in the notebook

## Dataset

Bank marketing dataset with customer/campaign fields:

`age, job, marital, education, default, balance, housing, loan, contact, day, month, duration, campaign, pdays, previous, poutcome, y`

Target variable `y` indicates whether the customer subscribed to a term deposit. The classes are imbalanced (~88% "no" vs ~12% "yes").

## Project Structure

**Part 1 — Data Storage & Management (Hadoop/Hive)**
Loads `bank.csv` with an explicit schema, writes it into a managed Hive database/table (`banking_dw.customer_transactions`), and runs HiveQL queries: overall subscription rate, average balance and subscription rate by job, and campaign effectiveness by prior outcome.

**Part 2 — Exploratory Data Analysis**
Uses the Spark DataFrame API (not pandas) for schema/shape checks, summary statistics, null and "unknown"-value audits, class balance, subscription rate by category, numeric feature means by target class, a correlation matrix, and a couple of small matplotlib visualizations (age distribution, subscription rate by month).

**Part 3 — Predictive Modeling (Spark ML)**
Builds a Spark ML `Pipeline` (StringIndexer → OneHotEncoder → VectorAssembler → classifier) to predict `y`. Trains and evaluates two models on an 80/20 split:
- Logistic Regression
- Random Forest (100 trees, max depth 8)

Evaluated on AUC, accuracy, F1, precision, and recall (accuracy alone is misleading given class imbalance), plus a confusion matrix and Random Forest feature importances.

**Part 4 — Real-Time Transaction Analysis (Spark Structured Streaming)**
Simulates a live transaction feed by drip-feeding 10 shuffled batch files into a watched folder (`readStream`), computing a rolling subscribe-rate/average-balance aggregation per job type into an in-memory sink. Includes an optional second stream sketching a simplified high-value-transaction alert rule.

**Part 5 — Data Parallelism**
Demonstrates repartitioning vs. coalescing, measures the speedup from caching a DataFrame reused across repeated aggregations, and shows a broadcast join (small job→segment lookup table shipped to every executor instead of shuffling the large customer table).

## Key Findings

- Class imbalance (~88/12) means AUC/F1 are more meaningful than raw accuracy.
- `duration` (last call length) is strongly predictive but only known *after* a call — a real "predict before calling" model would typically exclude it.
- Prior campaign success (`poutcome = success`) is one of the strongest positive signals.
- Students, retired people, and management-level jobs show higher subscription rates than blue-collar jobs.
- Random Forest generally outperforms Logistic Regression by capturing non-linear feature interactions.
- Caching pays off for DataFrames reused across multiple actions; broadcast joins avoid shuffling the large table when one side of a join is small.

## Running the Notebook

1. Open the notebook in Google Colab.
2. Run the setup cell to install PySpark and start the Spark session.
3. Upload `bank.csv` when prompted (or mount Drive).
4. Run Parts 1–3 sequentially.
5. For Part 4, upload the `streaming_chunks/` folder, or run the provided helper cell to regenerate the 10 batch files from `bank.csv` before starting the stream.
6. Run Part 5 for the data-parallelism benchmarks.

## Possible Production Extensions

- Replace the CSV-drip simulation in Part 4 with a real Kafka topic.
- Deploy on a managed cluster (EMR / Dataproc / Databricks) instead of Colab's single node.
- Add MLflow model tracking and a feature store for the Spark ML pipeline.
- Add data quality checks (e.g. Great Expectations) before writes into the Hive warehouse.
- Tune `spark.sql.shuffle.partitions` and executor sizing for real cluster hardware.
