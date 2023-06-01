variable "name" {
  type = string
  description = "Name of the environment"
}

variable "gcloud_project" {
  type = string
  description = "Name of the gcloud project hosting the environment"
}

variable "digest" {
  type = string
  description = "SHA256 digest of the graphql application image to deploy"
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