resource "google_bigquery_dataset" "event_monitoring" {
  dataset_id = "event_monitoring"
  friendly_name = "Event Monitoring Dataset"
  location = var.bigquery_dataset_location
}

resource "google_bigquery_dataset_iam_member" "editor" {
  dataset_id = google_bigquery_dataset.event_monitoring.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.looker-sa.email}"
}

resource "google_project_iam_member" "jobUser" {
  role       = "roles/bigquery.jobUser"
  member     = "serviceAccount:${google_service_account.looker-sa.email}"
}

output "event-monitoring-dataset" {
  value = google_bigquery_dataset.event_monitoring.dataset_id
}

resource "google_bigquery_table" "invalid_messages" {
  deletion_protection = false
  dataset_id = google_bigquery_dataset.event_monitoring.dataset_id
  table_id = "invalid_messages"
  description = "Failed PubSub payloads"
  time_partitioning {
    type = "DAY"
    field = "published_ts"
  }
  schema = <<EOF
[
  {
    "mode": "REQUIRED",
    "name": "published_ts",
    "type": "TIMESTAMP"
  },
  {
    "mode": "REQUIRED",
    "name": "failure_reason",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "message_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "payload_string",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "payload_bytes",
    "type": "BYTES"
  },
  {
    "mode": "REPEATED",
    "name": "attributes",
    "type": "RECORD",
    "fields": [
            {
                "name": "name",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "value",
                "type": "STRING",
                "mode": "REQUIRED"
            }
    ]
  }
]
EOF
}

resource "google_bigquery_table" "process_info" {
  deletion_protection = false
  dataset_id = google_bigquery_dataset.event_monitoring.dataset_id
  table_id = "process_info"
  description = "Details about processes"

  schema = <<EOF
[
  {
    "mode": "NULLABLE",
    "name": "user_id",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "process_name",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "category",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "description",
    "type": "STRING"
  }
]
EOF
}

resource "google_bigquery_table" "events" {
  deletion_protection = false
  dataset_id = google_bigquery_dataset.event_monitoring.dataset_id
  table_id = "events"
  description = "All events"
  time_partitioning {
    type = "DAY"
    field = "request_ts"
  }
  schema = <<EOF
[
  {
    "mode": "REQUIRED",
    "name": "request_ts",
    "type": "TIMESTAMP"
  },
  {
    "mode": "REQUIRED",
    "name": "bytes_sent",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "bytes_received",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "dst_hostname",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "dst_ip",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "dst_port",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "src_ip",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "user_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "process_name",
    "type": "STRING"
  }
]
EOF

}

resource "google_bigquery_table" "findings" {
  deletion_protection = false
  dataset_id = google_bigquery_dataset.event_monitoring.dataset_id
  table_id = "findings"
  description = "Suspicious activity"
  time_partitioning {
    type = "DAY"
    field = "request_ts"
  }
  schema = <<EOF
[
  {
    "mode": "REQUIRED",
    "name": "request_ts",
    "type": "TIMESTAMP"
  },
  {
    "mode": "REQUIRED",
    "name": "type",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "source_ip",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "user_id",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "level",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "description",
    "type": "STRING"
  }
]
EOF

}