resource "google_service_account" "looker-sa" {
  account_id   = "looker-sa"
  display_name = "Service Account for Looker"
}
