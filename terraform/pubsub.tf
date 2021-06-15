resource "google_pubsub_topic" "event_topic" {
  name = "event-topic"
}

output "event-topic" {
  value = google_pubsub_topic.event_topic.id
}

resource "google_pubsub_subscription" "event_sub" {
  name = "event-sub"
  topic = google_pubsub_topic.event_topic.name
}

output "event-sub" {
  value = google_pubsub_subscription.event_sub.id
}

//resource "google_pubsub_topic" "another_event_topic" {
//  name = "another-event-topic"
//}
//
//resource "google_pubsub_subscription" "another_event_sub" {
//  name = "another_event_sub"
//  topic = google_pubsub_topic.another_event_topic.name
//}

resource "google_pubsub_topic" "suspicious_activity_topic" {
  name = "suspicious-activity-topic"
}

output "suspicious-activity-topic" {
  value = google_pubsub_topic.suspicious_activity_topic.id
}

resource "google_pubsub_subscription" "suspicious_activity_sub" {
  name = "suspicious-activity-sub"
  topic = google_pubsub_topic.suspicious_activity_topic.name
}

output "suspicious-activity-sub" {
  value = google_pubsub_subscription.suspicious_activity_sub.id
}