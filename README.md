# Beam Pipelines - Streaming Analytics Techniques

This project is a demo for several Beam techniques to do streaming analytics.

## Running the demo
1. Create a GCP project
1. Create a file in `terraform` directory named `terraform.tfvars` with the following content:
    ```hcl-terraform
    project_id = "<GCP Project Id>"
    ``` 
    There are additional Terraform variables that can be overwritten; see [variables.tf](terraform/variables.tf) for details.
1. Run the following commands:
    ```shell script
    export PROJECT_ID=<project-id>
    export GCP_REGION=us-central1
    export BIGQUERY_REGION=us-central1
    ```
1. Create BigQuery tables, Pub/Sub topics and subscriptions, and GCS buckets by running this script:
    ```shell script
    source ./setup-env.sh
    ```
1. Start event generation process:
    ```shell script
    ./start-event-generation.sh
    ```
1. Start the event processing pipeline:
    ```shell script
    (cd pipeline; ./run-streaming-pipeline.sh)
    ```
1. Optionally, start the pipeline which will ingest the findings sent as pubsub messages into BigQuery:
    ```shell script
    ./start-findings-to-bigquery-pipeline.sh
    ```
   
## Cleaning up
1. Shutdown the pipelines via GCP console (TODO: add scripts)
1. Run this command:
    ```shell script
    cd terraform; terraform destroy
    ```
Alternatively, delete the project you created.

#Disclaimer
The techniques and code contained here are not supported by Google and is provided as-is 
(under Apache license). This repo provides some options you can investigate, evaluate and employ 
if you choose to.


