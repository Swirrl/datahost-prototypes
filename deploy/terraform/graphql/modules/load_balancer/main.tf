locals {
  health_check_name = "${var.env}-health-check"
  backend_service_name = "${var.env}-backend-service"
  instance_group_name = "${var.env}-instance-group"
  https_url_map_name = "${var.env}-https-load-balancer"
  http_url_map_name = "${var.env}-http-load-balancer"
  address_name = "${var.env}-lb-address"
  https_global_forwarding_rule_name = "${var.env}-https-forwarding-rule"
  http_global_forwarding_rule_name = "${var.env}-http-forwarding-rule"
  https_proxy_name = "${var.env}-https-proxy"
  http_proxy_name = "${var.env}-http-proxy"

  ssl_policy_name = "${var.env}-ssl-policy"
  graphql_certificate_name = "${var.env}-graphql-certificate"

  fqdn = "${var.dns.host}.${var.dns.zone}"
}

# backend
resource "google_compute_health_check" "lb_health_check" {
  name = local.health_check_name
  http_health_check {
    port = "80"
    request_path = "/ide"
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
  name = local.graphql_certificate_name
  managed {
    domains = [aws_route53_record.graphql_record.name]
  }
}

data "aws_route53_zone" "graphql_zone" {
  name = var.dns.zone
}

resource "aws_route53_record" "graphql_record" {
  zone_id = data.aws_route53_zone.graphql_zone.zone_id
  name = "graphql-prototype.gss-data.org.uk"
  type = "A"
  ttl = 300
  records = [google_compute_global_address.lb_address.address]
}