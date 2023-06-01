variable "name" {
  type = string
  description = "Name of the server to create"
}

variable "digest" {
  type = string
  description = "SHA256 digest of the graphql application image to deploy"
}