"""grocery_daily_batch - nightly batch orchestration (Airflow 2.7+ / Cloud Composer 2).

    wait for 6 CSV feeds -> validate headers -> Dataflow loads (dimensions || facts)
        -> data-quality gates -> BigQuery models -> (weekly) retrain -> forecast & inventory plan
        -> customer segments / recommendations -> e-mail campaign -> run summary

Where to look in the Airflow console (Composer: Environments > <env> > "Airflow webserver"):
  Grid view    : one column per run, one row per task; red = failed, orange = upstream_failed, pink = skipped.
  Graph view   : the dependency graph below. Click a task > Log for the Dataflow job id / BigQuery job id.
  Task groups  : `wait_for_files` (6 sensors) and `data_quality` (checks) expand in the graph.
  Admin>Variables: runtime switches (send_emails, force_retrain, alert_email, dq_*).

Static infra settings come from environment variables (Composer: Environment variables):
  GCP_PROJECT, GCS_BUCKET, GCP_REGION (default asia-south1), DATAFLOW_SA (optional worker service account).
Landing convention: gs://$GCS_BUCKET/landing/<ds>/<feed>.csv  where <ds> is the run's logical date (YYYY-MM-DD) -
a run that fires at 02:00 on the 8th processes the files exported for the 7th.
"""
import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# The project code (pipeline/, campaign/, sql/, setup.py) is deployed to <dags>/grocery_platform/
PLATFORM_DIR = Path(os.environ.get("GROCERY_PLATFORM_DIR", Path(__file__).parent / "grocery_platform"))
sys.path.insert(0, str(PLATFORM_DIR))

PROJECT = os.environ.get("GCP_PROJECT", "my-project")
BUCKET = os.environ.get("GCS_BUCKET", "my-grocery-bucket")
REGION = os.environ.get("GCP_REGION", "asia-south1")
DATAFLOW_SA = os.environ.get("DATAFLOW_SA")
DATASET = "grocery"
IST = pendulum.timezone("Asia/Kolkata")

# feed -> required columns (taken from the same schema definitions the Dataflow job uses)
from pipeline.schemas import FEEDS  # noqa: E402

REQUIRED_COLUMNS = {name: [c.split(":")[0] for c in f["bq_schema"].split(",")] for name, f in FEEDS.items()}
DIMENSION_FEEDS = ["products", "customers", "festivals", "expenses"]
FACT_FEEDS = ["transactions", "availability"]
LANDING = "landing/{{ ds }}"


def read_sql(name):
    return (PLATFORM_DIR / "sql" / name).read_text()


def bq_job(task_id, sql_file, **kw):
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={"query": {"query": read_sql(sql_file), "useLegacySql": False}},
        project_id=PROJECT,
        location=REGION,
        **kw,
    )


# ---------------------------------------------------------------- callbacks
def notify_failure(context):
    """Runs when a task fails after all retries. Always logs; e-mails if the `alert_email` Variable is set."""
    ti = context["task_instance"]
    msg = (f"Task {ti.dag_id}.{ti.task_id} failed (run {context['run_id']}, try {ti.try_number}). "
           f"Log: {ti.log_url}")
    logging.error(msg)
    to = Variable.get("alert_email", default_var="")
    if to:
        try:
            from airflow.utils.email import send_email
            send_email(to=[x.strip() for x in to.split(",")], subject=f"[Grocery pipeline] FAILED: {ti.task_id}", html_content=msg)
        except Exception:  # noqa: BLE001 - alerting must never mask the real failure
            logging.exception("could not send alert e-mail (check SMTP / SendGrid setup)")


def sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    logging.error("SLA missed for: %s", task_list)


# ---------------------------------------------------------------- python callables
def validate_headers(ds, **_):
    """Fail fast (no retries) if a feed is missing required columns - cheaper than a failed Dataflow job."""
    client = GCSHook().get_conn()
    problems = []
    for feed, cols in REQUIRED_COLUMNS.items():
        blob = client.bucket(BUCKET).blob(f"landing/{ds}/{FEEDS[feed]['file']}")
        head = blob.download_as_bytes(start=0, end=8192).decode("utf-8", "replace").splitlines()[0]
        have = {c.strip() for c in head.split(",")}
        missing = [c for c in cols if c not in have]
        if missing:
            problems.append(f"{feed}: missing {missing}")
        logging.info("%s header OK" if not missing else "%s header BAD", feed)
    if problems:
        raise AirflowFailException("; ".join(problems))


def quality_check(check_name, sql, **_):
    """Run a query that returns one boolean. False/NULL/error => the task fails and everything downstream is skipped."""
    client = BigQueryHook().get_client(project_id=PROJECT)
    row = next(iter(client.query(sql, location=REGION).result()), None)
    ok = bool(row and row[0])
    logging.info("data-quality check %s -> %s", check_name, "PASS" if ok else "FAIL")
    if not ok:
        raise AirflowFailException(f"Data-quality check failed: {check_name}\nSQL: {sql}")


def decide_training(**_):
    """Retrain weekly (Sunday), when forced, or when the model does not exist yet."""
    if Variable.get("force_retrain", default_var="false").lower() == "true":
        return "train_model"
    if pendulum.now(IST).weekday() == 6:  # datetime.weekday(): Monday = 0 ... Sunday = 6
        return "train_model"
    try:
        BigQueryHook().get_client(project_id=PROJECT).get_model(f"{PROJECT}.{DATASET}.demand_forecast_model")
        return "skip_training"
    except Exception:  # noqa: BLE001 - NotFound or anything else: train to be safe
        return "train_model"


def run_campaign(**_):
    from campaign.send_emails import main
    live = Variable.get("send_emails", default_var="false").lower() == "true"
    args = ["--project", PROJECT] + ([] if live else ["--dry_run", "--limit", "5"])
    logging.info("campaign mode: %s", "LIVE" if live else "DRY RUN (set Variable send_emails=true to send)")
    main(args)


def run_summary(**_):
    client = BigQueryHook().get_client(project_id=PROJECT)
    sql = f"""
    SELECT (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.raw_transactions`) AS transactions,
           (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.raw_customers`) AS customers,
           (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.pipeline_dead_letter`
              WHERE loaded_at >= DATETIME_SUB(CURRENT_DATETIME('UTC'), INTERVAL 12 HOUR)) AS rejected_rows_last_12h,
           (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.inventory_plan` WHERE suggested_order_qty > 0) AS products_to_reorder,
           (SELECT COUNT(DISTINCT customer_id) FROM `{PROJECT}.{DATASET}.v_email_campaign_queue`) AS customers_in_campaign"""
    row = dict(next(iter(client.query(sql, location=REGION).result())).items())
    logging.info("RUN SUMMARY: %s", row)
    return row


# ---------------------------------------------------------------- DAG
default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="grocery_daily_batch",
    description="CSV landing -> Dataflow -> BigQuery -> forecast/recommendations -> e-mail",
    start_date=pendulum.datetime(2026, 10, 1, tz=IST),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    default_args=default_args,
    sla_miss_callback=sla_miss,
    tags=["grocery", "batch", "gcp"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    # ---- 1. warehouse bootstrap (idempotent) -------------------------------------------------
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET, project_id=PROJECT, location=REGION, exists_ok=True)
    create_dead_letter = BigQueryInsertJobOperator(
        task_id="create_dead_letter_table",
        configuration={"query": {"useLegacySql": False, "query": f"""
            CREATE TABLE IF NOT EXISTS `{PROJECT}.{DATASET}.pipeline_dead_letter`
            (feed STRING, raw_line STRING, error STRING, loaded_at DATETIME)
            PARTITION BY DATE(loaded_at)"""}},
        project_id=PROJECT, location=REGION)

    # ---- 2. wait for the exports (sensors free their worker slot between pokes) -------------
    with TaskGroup("wait_for_files") as wait_for_files:
        for feed, spec in FEEDS.items():
            GCSObjectExistenceSensor(
                task_id=f"wait_{feed}", bucket=BUCKET, object=f"{LANDING}/{spec['file']}",
                mode="reschedule", poke_interval=300, timeout=4 * 3600,
                retries=0)  # a missing file is an upstream problem, not something a retry fixes

    check_headers = PythonOperator(task_id="validate_headers", python_callable=validate_headers)

    # ---- 3. Dataflow loads: reference data and facts run in parallel -------------------------
    def dataflow_load(task_id, feeds, write_mode):
        opts = {
            "project": PROJECT, "region": REGION, "runner": "DataflowRunner",
            "input_dir": f"gs://{BUCKET}/{LANDING}", "dataset": DATASET, "feeds": ",".join(feeds),
            "write_mode": write_mode,
            "temp_location": f"gs://{BUCKET}/tmp", "staging_location": f"gs://{BUCKET}/staging",
            "setup_file": str(PLATFORM_DIR / "setup.py"),
            "max_num_workers": "6",
        }
        if DATAFLOW_SA:
            opts["service_account_email"] = DATAFLOW_SA
        return DataflowCreatePythonJobOperator(
            task_id=task_id,
            py_file=str(PLATFORM_DIR / "pipeline_launcher.py"),
            job_name=f"grocery-{task_id.replace('_', '-')}-{{{{ ds_nodash }}}}",
            options=opts, py_requirements=["apache-beam[gcp]==2.60.0"], py_interpreter="python3",
            py_system_site_packages=False, location=REGION, project_id=PROJECT,
            poll_sleep=30, retries=1)

    gate_load = EmptyOperator(task_id="ready_to_load")
    # Reference data is small: full reload each day. Facts: full reload too until you move to daily increments
    # (switch to WRITE_APPEND + MERGE in 01_models.sql when the history gets large).
    load_dimensions = dataflow_load("load_dimensions", DIMENSION_FEEDS, "WRITE_TRUNCATE")
    load_facts = dataflow_load("load_facts", FACT_FEEDS, "WRITE_TRUNCATE")

    # ---- 4. data-quality gates: any failure stops the pipeline BEFORE bad data reaches reports -
    CHECKS = {
        "transactions_not_empty": f"SELECT COUNT(*) > 0 FROM `{PROJECT}.{DATASET}.raw_transactions`",
        "no_duplicate_transaction_ids": f"""SELECT COUNT(*) = 0 FROM (
            SELECT transaction_id FROM `{PROJECT}.{DATASET}.raw_transactions` GROUP BY 1 HAVING COUNT(*) > 1)""",
        "reject_ratio_below_1pct": f"""SELECT IFNULL(SAFE_DIVIDE(
            (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.pipeline_dead_letter`
               WHERE feed = 'transactions' AND loaded_at >= DATETIME_SUB(CURRENT_DATETIME('UTC'), INTERVAL 6 HOUR)),
            (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.raw_transactions`)), 0) < 0.01""",
        "data_is_fresh": f"""SELECT DATE_DIFF(DATE '{{{{ ds }}}}', MAX(DATE(transaction_ts)), DAY)
            <= {{{{ var.value.get('dq_max_staleness_days', 2) | int }}}}
            FROM `{PROJECT}.{DATASET}.raw_transactions`""",
        "no_orphan_products": f"""SELECT IFNULL(SAFE_DIVIDE(COUNTIF(p.product_id IS NULL), COUNT(*)), 0) < 0.001
            FROM `{PROJECT}.{DATASET}.raw_transactions` t
            LEFT JOIN `{PROJECT}.{DATASET}.raw_products` p USING (product_id)""",
        "availability_not_empty": f"SELECT COUNT(*) > 0 FROM `{PROJECT}.{DATASET}.raw_availability`",
    }
    with TaskGroup("data_quality") as data_quality:
        for name, sql in CHECKS.items():
            PythonOperator(
                task_id=f"dq_{name}", python_callable=quality_check,
                op_kwargs={"check_name": name, "sql": sql},  # op_kwargs is templated, so {{ ds }} / {{ var.* }} resolve
                retries=0)  # a failed check is a data problem; retrying hides it

    # ---- 5. transformations -----------------------------------------------------------------
    build_models = bq_job("build_models", "01_models.sql")
    pick_training = BranchPythonOperator(task_id="decide_training", python_callable=decide_training)
    train_model = bq_job("train_model", "02a_train_model.sql", execution_timeout=timedelta(hours=2))
    skip_training = EmptyOperator(task_id="skip_training")
    forecast_and_plan = bq_job("forecast_and_plan", "02b_forecast_and_plan.sql",
                               trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    customer_models = bq_job("customer_models", "03_customers.sql")

    # ---- 6. activation -----------------------------------------------------------------------
    email_campaign = PythonOperator(task_id="email_campaign", python_callable=run_campaign, retries=0)
    summary = PythonOperator(task_id="run_summary", python_callable=run_summary,
                             sla=timedelta(hours=5))  # whole pipeline should finish by 07:00 IST; misses show in Browse > SLA Misses

    # ---- dependencies (this is what the Graph view draws) -------------------------------------
    start >> [create_dataset, wait_for_files]
    create_dataset >> create_dead_letter
    wait_for_files >> check_headers
    [create_dead_letter, check_headers] >> gate_load >> [load_dimensions, load_facts]
    [load_dimensions, load_facts] >> data_quality >> build_models >> pick_training
    pick_training >> [train_model, skip_training] >> forecast_and_plan
    forecast_and_plan >> customer_models >> email_campaign >> summary >> end
