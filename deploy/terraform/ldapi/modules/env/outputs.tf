output "basic_auth_password_secret_name" {
  description = "Name of the basic auth secret password to configure manually"
  value = module.server.basic_auth_password_secret_name
}