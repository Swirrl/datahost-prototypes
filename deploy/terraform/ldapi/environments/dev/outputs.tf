output "basic_auth_password_secret_name" {
  description = "Name of the basic auth secret password to configure manually"
  value = module.dev.basic_auth_password_secret_name
}