#!/usr/bin/env bash
# Deploy to Cloud Composer (managed Apache Airflow) and print the Airflow console URL.
# Usage: PROJECT=my-proj BUCKET=my-grocery-bucket ./scripts/deploy_composer.sh
# First run creates the environment (takes ~25 min); later runs only upload code.
set -euo pipefail
: "${PROJECT:?}" "${BUCKET:?}"
REGION="${REGION:-asia-south1}"
ENV_NAME="${ENV_NAME:-grocery-airflow}"
IMAGE="${COMPOSER_IMAGE:-composer-2.9.7-airflow-2.9.3}"   # any Composer 2 / Airflow >= 2.7 image works
DATAFLOW_SA="${DATAFLOW_SA:-grocery-dataflow@${PROJECT}.iam.gserviceaccount.com}"
gcloud config set project "$PROJECT" >/dev/null

if ! gcloud composer environments describe "$ENV_NAME" --location "$REGION" >/dev/null 2>&1; then
  gcloud composer environments create "$ENV_NAME" --location "$REGION" --image-version "$IMAGE" \
    --environment-size small \
    --env-variables "GCP_PROJECT=$PROJECT,GCS_BUCKET=$BUCKET,GCP_REGION=$REGION,DATAFLOW_SA=$DATAFLOW_SA"

  # The Composer service account launches Dataflow jobs and runs BigQuery jobs
  CSA=$(gcloud composer environments describe "$ENV_NAME" --location "$REGION" --format='value(config.nodeConfig.serviceAccount)')
  for role in roles/composer.worker roles/dataflow.developer roles/bigquery.dataEditor roles/bigquery.jobUser roles/storage.objectAdmin; do
    gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$CSA" --role="$role" --quiet >/dev/null
  done
  gcloud iam service-accounts add-iam-policy-binding "$DATAFLOW_SA" \
    --member="serviceAccount:$CSA" --role=roles/iam.serviceAccountUser --quiet >/dev/null
fi

# Stage:  dags/grocery_daily_batch.py  +  dags/grocery_platform/{pipeline,campaign,sql,setup.py,pipeline_launcher.py}
STAGE=$(mktemp -d)
mkdir -p "$STAGE/grocery_platform"
cp dags/grocery_daily_batch.py "$STAGE/"
cp -r pipeline campaign sql setup.py pipeline_launcher.py "$STAGE/grocery_platform/"
find "$STAGE" -name __pycache__ -prune -exec rm -rf {} +
gcloud composer environments storage dags import --environment "$ENV_NAME" --location "$REGION" --source "$STAGE"
rm -rf "$STAGE"

# Safe defaults: e-mails are a dry run until you flip this on
gcloud composer environments run "$ENV_NAME" --location "$REGION" variables -- set send_emails false
gcloud composer environments run "$ENV_NAME" --location "$REGION" variables -- set alert_email "${ALERT_EMAIL:-}"

echo
echo "Airflow console: $(gcloud composer environments describe "$ENV_NAME" --location "$REGION" --format='value(config.airflowUri)')"
echo "Trigger a run:   gcloud composer environments run $ENV_NAME --location $REGION dags -- trigger grocery_daily_batch"
