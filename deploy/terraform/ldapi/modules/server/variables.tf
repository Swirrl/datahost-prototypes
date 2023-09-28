variable "name" {
  type = string
  description = "Name of the server to create"
}

variable "gcloud_project" {
  type = string
  description = "Name of the host gcloud project for the server"
}

variable "digest" {
  type = string
  description = "SHA256 digest of the ldapi application container to deploy"
}

variable "zone" {
  type = string
  description = "Zone to host the server in"
}