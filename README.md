# Grocery Data Platform (GCP batch: Dataflow → BigQuery → Looker, orchestrated by Airflow)

## Architecture

```
POS / billing export (CSV, daily)        Apache Airflow  (Cloud Composer in production)
        │                                dags/grocery_daily_batch.py - runs 02:00 IST
        ▼
 GCS  gs://BUCKET/landing/YYYY-MM-DD/
        │
        ├─ wait_for_files   6 sensors (one per feed)
        ├─ validate_headers fail fast on missing columns
        ├─ load_dimensions ║ load_facts      Dataflow (Beam) jobs in parallel
        │       bad rows ► grocery.pipeline_dead_letter
        ├─ data_quality     6 gates: not empty, no duplicates, reject ratio, freshness, orphans, availability
        ├─ build_models     sql/01_models.sql      fact_sales, P&L, peak hours, daily demand
        ├─ decide_training  weekly (Sun) or if model missing ─► train_model (sql/02a)  | skip_training
        ├─ forecast_and_plan sql/02b   45-day forecast + festival uplift + inventory plan
        ├─ customer_models  sql/03     RFM, affinity, recommendations, offers
        ├─ email_campaign   dry run until Variable send_emails=true
        └─ run_summary      counts logged for the console
                      │
                      ▼
                BigQuery  ──►  Looker dashboards
```

## Input feeds (CSV, headers must match)

| File | Columns |
|---|---|
| transactions.csv | transaction_id, order_id, customer_id, product_id, quantity, unit_price, discount_pct, payment_mode, transaction_ts |
| customers.csv | customer_id, name, email, phone, city, signup_date, marketing_opt_in |
| products.csv | product_id, product_name, category, cost_price, selling_price, supplier, shelf_life_days |
| availability.csv | snapshot_date, product_id, opening_stock, closing_stock, stockout_flag, reorder_level |
| expenses.csv | expense_id, expense_date, category, amount |
| festivals.csv | festival, start_date, end_date (you maintain this yearly: Diwali, Ganesh Chaturthi, Ugadi, ...) |

If your real export uses other column names, change only `pipeline/schemas.py`.

## Step 1: build and test with open-source data

The test data comes from the open **dunnhumby "Complete Journey"** grocery dataset (real supermarket baskets), packaged on
GitHub at <https://github.com/cunningjames/completejourney_py>. It is for research/testing use, so use it to build and test only.

```bash
python scripts/prepare_open_data.py --fetch --out data/open     # or --source <folder with the parquet files>
```

| | |
|---|---|
| **Real** (from the source) | transactions (household, basket, product, qty, price, discount, timestamp), product hierarchy, household ids |
| **Simulated** (not in the source, seeded) | cost_price (department margin ≈ 25%), shelf life, stock availability (reorder policy driven by real daily demand), operating expenses, e-mail addresses, opt-in flags, festival calendar |
| **Transformed** | dates shifted by whole weeks so the data ends yesterday (so recency, "next festival" and forecasts behave like live data); top 500 products kept so BQML forecasting stays cheap; fuel/coupon lines dropped |

Things to know when reading results from this data: it is a **US** store (values are in dollars; the festival calendar holds
US holidays at the shifted dates, not Diwali), it covers **one year** (yearly seasonality is not learnable from it, so the
forecast leans on the explicit festival uplift), and the **latest month is partial** so it can show a LOSS because a full
month of expenses is compared with a few days of sales.

Result of the conversion: 385,125 transactions, 2,432 customers, 500 products, 183,000 availability rows, 25.5% gross margin;
every row passes the pipeline parsers (0 rejects, 0 duplicate ids, 0 orphan keys).

## Step 2: run it on GCP

```bash
PROJECT=my-proj BUCKET=my-grocery-bucket ./scripts/setup_gcp.sh
gsutil -m cp data/open/*.csv gs://my-grocery-bucket/landing/$(date +%F)/
```

You can run the pieces by hand first (Dataflow, then `bq query < sql/01_models.sql` …), or let Airflow do it all (next step).

## Step 3: Airflow orchestration

**Production: Cloud Composer** (managed Airflow; its "Airflow webserver" link is your console)

```bash
PROJECT=my-proj BUCKET=my-grocery-bucket ALERT_EMAIL=you@shop.com ./scripts/deploy_composer.sh
# prints the Airflow console URL; first run creates the environment (~25 min)
```

**Local console for development:** `cp .env.example .env`, put a service-account key at `keys/sa.json`, then
`docker compose up -d` and open <http://localhost:8080> (admin / admin). It talks to your real GCP project.

### What to check in the console

| Where | What you see |
|---|---|
| **DAGs list** | `grocery_daily_batch`, last-run status, next run. Unpause it with the toggle. |
| **Grid view** | One column per daily run, one row per task. Green = success, red = failed, orange = upstream failed, pink = skipped (normal for `skip_training`). |
| **Graph view** | The dependency graph above. Click a task → **Log** for the Dataflow job id and BigQuery job id. |
| **Task groups** | `wait_for_files` and `data_quality` expand to show each sensor / check. A red `dq_*` task tells you exactly which rule failed. |
| **Browse → SLA Misses** | Runs that did not finish by 07:00 IST. |
| **Admin → Variables** | `send_emails` (true = really send), `force_retrain`, `alert_email`, `dq_max_staleness_days`. |

Common actions: **Trigger DAG** (play button) for a manual run; select a failed task → **Clear** to rerun from that point
(downstream tasks follow); `airflow dags backfill` for past dates. File sensors wait up to 4 hours for the exports and do not retry
(a missing file is an upstream problem); Dataflow loads retry once; data-quality checks never retry.

Failure alerts: every final task failure is logged, and e-mailed to `alert_email` if SMTP/SendGrid is configured on the environment.

Static settings come from environment variables on the Composer environment (`GCP_PROJECT`, `GCS_BUCKET`, `GCP_REGION`, `DATAFLOW_SA`).
E-mail credentials (`SMTP_HOST`, `SMTP_USER`, `SMTP_PASSWORD`) belong in Secret Manager / environment secrets, not in the repo.

## What answers which business question

| Question | Where |
|---|---|
| Profit / loss per month, margin, opex | `v_monthly_pnl`, `v_expense_breakdown` |
| Which products lose money / earn most | `v_product_profitability` |
| Peak business hours | `v_hourly_traffic`, `v_hour_weekday_heatmap`, `v_peak_hour_top_products` |
| Stockouts and low stock | `v_stockout_summary`, `v_restock_alerts` |
| Festival inventory (how much to order) | `demand_forecast`, `v_festival_uplift`, `inventory_plan` |
| Customer segments | `customer_rfm` |
| Personalised recommendations + discounts | `customer_recommendations`, `customer_offers` |
| Win-back / new-customer offers | `customer_offers`, `v_acquisition_hooks`, `v_bundle_ideas` |
| Clear slow-moving stock | `v_clearance_candidates` |
| Retention and area growth | `v_cohort_retention`, `v_city_performance` |

## Growth ideas the data supports

1. **Peak hours:** staff counters and restock shelves ahead of the busiest hours (`v_peak_hour_top_products`); use quiet hours for time-limited offers to flatten demand.
2. **Festival stocking:** order from `inventory_plan` 3-4 weeks ahead; target stock = upper forecast × historical uplift; perishables are flagged.
3. **Win-back:** lapsed segments get bigger, margin-capped discounts; Champions get small rewards (protects profit).
4. **New customers:** use the highest `loyalty_conversion` items from `v_acquisition_hooks` as welcome offers; push combos from `v_bundle_ideas`.
5. **Margin protection:** discounts never take a product below 5% margin; review `is_loss_making` products monthly.
6. **Wastage:** discount `v_clearance_candidates` before expiry.

## Verified vs not verified

**Run and passing here:** the open-data conversion; all six feeds through the pipeline parsers (including bad-row rejection);
the DAG file imports and builds the expected graph (28 tasks, no cycles, ordering rules hold); data-quality and header-check
logic with fake clients.
**Not run here (the sandbox cannot install Apache Beam or Airflow and has no GCP access):** the Dataflow job itself, the BigQuery SQL,
the BQML forecast, the real Airflow DagBag, Composer deployment and e-mail sending. Expect small fixes on the first real run.
Run `pytest tests/test_dag.py` in an environment with Airflow installed (it then uses the real DagBag).

## Other notes

- Dataset `grocery`, region `asia-south1`; change in the scripts and `dags/grocery_daily_batch.py` if different.
- Forecasting trains one ARIMA_PLUS series per product; limit it to active SKUs in production to control BigQuery ML cost.
- Daily loads currently reload each table (`WRITE_TRUNCATE`). When history grows, switch facts to `WRITE_APPEND` plus a MERGE in `01_models.sql`.
- E-mails go only to opted-in customers, at most once per 7 days, with an unsubscribe line. Check consent rules (India: DPDP Act) before large sends.
- `scripts/generate_sample_data.py` still creates purely synthetic data if you want a quick offline set.
