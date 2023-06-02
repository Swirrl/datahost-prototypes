variable "env" {
  type = string
  description = "Name of the environment"
}

variable "server_self_link" {
  type = string
  description = "Self link of the ldapi server instance"
}

variable "server_zone" {
  type = string
  description = "Zone of the ldapi server instance"
}

variable "dns" {
  type = object({
    host = string,
    zone = string
  })
}