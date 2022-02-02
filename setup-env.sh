#!/usr/bin/env bash
set -u

export TF_VAR_project_id=${PROJECT_ID}
export TF_VAR_region=${GCP_REGION}
export TF_VAR_bigquery_dataset_location=${BIGQUERY_REGION}

cd terraform
terraform init && terraform apply

export EVENT_GENERATOR_TEMPLATE=$(terraform output -raw event-generator-template)
export EVENT_GENERATOR_TEMPLATE_WITH_THREATS=$(terraform output -raw event-generator-template-with-threats)
export EVENT_TOPIC=$(terraform output -raw event-topic)
export EVENT_SUB=$(terraform output -raw event-sub)
export SUSPICIOUS_ACTIVITY_TOPIC=$(terraform output -raw suspicious-activity-topic)
export SUSPICIOUS_ACTIVITY_SUB=$(terraform output -raw suspicious-activity-sub)
export DATASET=$(terraform output -raw event-monitoring-dataset)
export DATAFLOW_TEMP_BUCKET=gs://$(terraform output -raw dataflow-temp-bucket)
export METADATA_BUCKET=$(terraform output -raw dataflow-temp-bucket)
export EVENTS_BUCKET=$(terraform output -raw events-bucket)
export REGION=$(terraform output -raw region)

cd ..
