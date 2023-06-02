variable "gcloud_project" {
  type = string
  description = "GCloud project to host the environment"
}

variable "name" {
  type = string
  description = "Name of the environment"
}

variable "digest" {
  type = string
  description = "SHA256 digest of the ldapi application container to deploy"
}

variable "gcloud_zone" {
  type = string
  description = "Zone to host the server in"
}

variable "dns" {
  type = object({
    host = string,
    zone = string
  })
  description = "DNS configuration for the graphql site"
}