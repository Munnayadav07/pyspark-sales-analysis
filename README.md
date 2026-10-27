# PySpark Sales Analysis

An end-to-end batch pipeline: **CSV -> clean -> revenue -> top products -> monthly trends -> charts**.

## Features

- Read CSV with an explicit schema (local, HDFS or S3 paths)
- Data cleaning: trimming, case normalization, multi-format dates, currency/percent parsing, validation, de-duplication
- Rejected rows are quarantined with a reason, not silently dropped
- Revenue: gross, discount, net
- Top-N products overall and per category (window functions)
- Monthly sales: revenue, orders, units, average order value, month-over-month growth
- Charts via Matplotlib, plus Parquet and CSV outputs

## Project structure

```
pyspark-sales-analysis/
├── data/raw/sales.csv              # sample dirty dataset (generated)
├── docs/
│   ├── architecture.md             # diagram + description
│   └── dataset_format.md           # column spec, cleaning rules
├── scripts/generate_sample_data.py
├── src/
│   ├── config.py                   # schema, date formats
│   ├── cleaning.py                 # read + clean + quarantine
│   ├── analysis.py                 # revenue, top products, monthly
│   ├── visualize.py                # charts
│   └── main.py                     # orchestration / CLI
├── tests/test_cleaning.py
└── requirements.txt
```

## Prerequisites

- Python 3.9+
- Java 8, 11, 17 or 21 (required by Spark)

## Setup and run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# (optional) regenerate sample data, 5,000 rows with injected dirt
python scripts/generate_sample_data.py --rows 5000

# run the pipeline
python src/main.py --input data/raw/sales.csv --output output --top-n 10

# or with spark-submit
spark-submit src/main.py --input data/raw/sales.csv --output output
```

### CLI options

| Flag | Default | Meaning |
|------|---------|---------|
| `--input` | `data/raw/sales.csv` | CSV file, directory or glob |
| `--output` | `output` | Output directory |
| `--top-n` | `10` | Number of top products |
| `--shuffle-partitions` | `8` | `spark.sql.shuffle.partitions` (raise for large data) |
| `--master` | none | e.g. `local[4]`; leave unset under `spark-submit` |

## Outputs

```
output/
├── cleaned/            # Parquet, partitioned by year/month
├── rejected/           # CSV: reject_reason, raw_record
├── reports/            # top_products, top_products_by_category, monthly_sales,
│                       # revenue_by_category, revenue_by_region (.csv)
└── charts/             # top_products.png, monthly_sales.png,
                        # revenue_by_category.png, revenue_by_region.png
```

## Dataset format

See [docs/dataset_format.md](docs/dataset_format.md). Minimum required columns:
`order_id, order_date, product_id, quantity, unit_price`.

**Revenue = quantity x unit_price x (1 - discount)**

## Tests

```bash
pytest -q
```

## Running on a cluster

```bash
spark-submit --master yarn --deploy-mode cluster \
  --py-files src/config.py,src/cleaning.py,src/analysis.py,src/visualize.py \
  src/main.py --input s3a://my-bucket/sales/*.csv --output s3a://my-bucket/sales-out \
  --shuffle-partitions 200
```

On a cluster, matplotlib and pandas are needed on the **driver** only. For S3, add the `hadoop-aws` package.

## Scaling and tuning notes

- Raise `--shuffle-partitions` (roughly 2-3x total cores) for large inputs; AQE coalesces small partitions.
- For very large CSVs, convert once to Parquet, since CSV is not splittable when compressed.
- If `product_id` is heavily skewed, enable `spark.sql.adaptive.skewJoin.enabled` (already the default in recent Spark).
- Orders and customers can be extended with joins to dimension tables. Use broadcast joins for small ones.

## Possible extensions

Customer RFM segmentation, Delta Lake/Iceberg output, Airflow scheduling, data-quality thresholds that fail the job, and a Streamlit dashboard over `output/reports`.

See [docs/architecture.md](docs/architecture.md) for the diagram and design decisions.
