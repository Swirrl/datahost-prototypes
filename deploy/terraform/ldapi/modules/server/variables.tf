variable "name" {
  type = string
  description = "Name of the server to create"
}

variable "digest" {
  type = string
  description = "SHA256 digest of the ldapi application container to deploy"
}

variable "zone" {
  type = string
  description = "Zone to host the server in"
}