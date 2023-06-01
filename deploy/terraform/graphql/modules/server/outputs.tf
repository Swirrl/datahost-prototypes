output "self_link" {
  description = "Self link of the graphql server instance"
  value = google_compute_instance.datahost_instance.self_link
}

output "zone" {
  description = "GCloud zone of the graphql server"
  value = google_compute_instance.datahost_instance.zone
}