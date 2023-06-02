locals {
  health_check_name = "${var.env}-ldapi-health-check"
  backend_service_name = "${var.env}-ldapi-backend-service"
  instance_group_name = "${var.env}-ldapi-instance-group"
  https_url_map_name = "${var.env}-ldapi-https-load-balancer"
  http_url_map_name = "${var.env}-ldapi-http-load-balancer"
  address_name = "${var.env}-ldapi-lb-address"
  https_global_forwarding_rule_name = "${var.env}-ldapi-https-forwarding-rule"
  http_global_forwarding_rule_name = "${var.env}-ldapi-http-forwarding-rule"
  https_proxy_name = "${var.env}-ldapi-https-proxy"
  http_proxy_name = "${var.env}-ldapi-http-proxy"

  ssl_policy_name = "${var.env}-ldapi-ssl-policy"
  certificate_name = "${var.env}-ldapi-certificate"

  fqdn = "${var.dns.host}.${var.dns.zone}"
}

# backend
resource "google_compute_health_check" "lb_health_check" {
  name = local.health_check_name
  http_health_check {
    port = "80"
    request_path = "/index.html"
  }
}

resource "google_compute_instance_group" "instance_group" {
  name = local.instance_group_name
  zone = var.server_zone

  instances = [var.server_self_link]

  named_port {
    name = "http"
    port = "80"
  }
}

resource "google_compute_backend_service" "backend_service" {
  name = local.backend_service_name
  health_checks = [google_compute_health_check.lb_health_check.id]
  protocol = "HTTP"
  port_name = "http"
  connection_draining_timeout_sec = 150
  session_affinity = "CLIENT_IP"

  backend {
    group = google_compute_instance_group.instance_group.self_link
  }
}

# frontend
resource "google_compute_global_address" "lb_address" {
  name = local.address_name
}

resource "google_compute_url_map" "https_url_map" {
  name = local.https_url_map_name
  default_service = google_compute_backend_service.backend_service.id
}

resource "google_compute_url_map" "http_url_map" {
  name = local.http_url_map_name
  default_url_redirect {
    https_redirect = true
    strip_query = false
  }
}

resource "google_compute_target_https_proxy" "https_proxy" {
  name = local.https_proxy_name
  url_map = google_compute_url_map.https_url_map.id
  ssl_certificates = [google_compute_managed_ssl_certificate.ssl_certificate.id]
  ssl_policy = google_compute_ssl_policy.lb_ssl_policy.id
}

resource "google_compute_target_http_proxy" "http_proxy" {
  name = local.http_proxy_name
  url_map = google_compute_url_map.http_url_map.id
}

resource "google_compute_global_forwarding_rule" "https_load_balancer" {
  name = local.https_global_forwarding_rule_name
  ip_protocol = "TCP"
  load_balancing_scheme = "EXTERNAL"
  port_range = "443"
  target = google_compute_target_https_proxy.https_proxy.id
  ip_address = google_compute_global_address.lb_address.id
}

resource "google_compute_global_forwarding_rule" "http_load_balancer" {
  name = local.http_global_forwarding_rule_name
  ip_protocol = "TCP"
  load_balancing_scheme = "EXTERNAL"
  port_range = "80"
  target = google_compute_target_http_proxy.http_proxy.id
  ip_address = google_compute_global_address.lb_address.id
}

# ssl
resource "google_compute_ssl_policy" "lb_ssl_policy" {
  name = local.ssl_policy_name
  profile = "MODERN"
  min_tls_version = "TLS_1_2"
}

resource "google_compute_managed_ssl_certificate" "ssl_certificate" {
  name = local.certificate_name
  managed {
    domains = [aws_route53_record.ldapi_record.name]
  }
}

data "aws_route53_zone" "dns_zone" {
  name = var.dns.zone
}

resource "aws_route53_record" "ldapi_record" {
  zone_id = data.aws_route53_zone.dns_zone.zone_id
  name = local.fqdn
  type = "A"
  ttl = 300
  records = [google_compute_global_address.lb_address.address]
}