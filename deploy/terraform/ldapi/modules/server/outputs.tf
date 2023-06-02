output "self_link" {
  description = "Self link of the ldapi server instance"
  value = google_compute_instance.datahost_ldapi_instance.self_link
}

output "zone" {
  description = "GCloud zone of the ldapi server"
  value = google_compute_instance.datahost_ldapi_instance.zone
}