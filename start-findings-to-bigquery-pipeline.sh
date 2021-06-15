set -e
set -u

JOB_NAME=findings-to-bigquery
gcloud dataflow jobs run ${JOB_NAME} \
    --gcs-location gs://dataflow-templates/latest/PubSub_Subscription_to_BigQuery \
    --project=${PROJECT_ID} \
    --region ${GCP_REGION} \
    --staging-location ${DATAFLOW_TEMP_BUCKET} \
    --parameters \
inputSubscription=${SUSPICIOUS_ACTIVITY_SUB},\
outputTableSpec=${PROJECT_ID}:${DATASET}.findings

