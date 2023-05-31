terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.64.0"
    }

    null = {
      source = "hashicorp/null"
      version = "3.1.1"
    }

    aws = {
      source = "hashicorp/aws"
      version = "5.0.1"
    }
  }

  # S3 backend state configuration
  backend "s3" {
    bucket = "swirrl-terraform-states"
    key = "datahost/dev/graphql.tfstate"
    region = "eu-west-2"

    dynamodb_table = "swirrl-terraform-locks"
    encrypt = true
  }
}

provider "google" {
  project = "swirrl-ons-datahost"
  region = "europe-west2"
}

provider "aws" {
  region = "eu-west-2"
}

locals {
  name = "tf-datahost-graphql"
  gcloud_zone = "europe-west2-a"
  service_account_id = "${local.name}-account"

  health_check_name = "lb-health-check"
  backend_service_name = "lb-backend-service"
  instance_group_name = "lb-instance-group"
  https_url_map_name = "https-load-balancer"
  http_url_map_name = "http-load-balancer"
  address_name = "lb-address"
  https_global_forwarding_rule_name = "lb-https-forwarding-rule"
  http_global_forwarding_rule_name = "lb-http-forwarding-rule"
  https_proxy_name = "lb-https-proxy"
  http_proxy_name = "lb-http-proxy"

  ssl_policy_name = "lb-ssl-policy"
  graphql_certificate_name = "graphql-certificate"
}

module "gce_container_spec" {
  source = "terraform-google-modules/container-vm/google"
  version = "~> 2.0"

  container = {
    image = "europe-west2-docker.pkg.dev/swirrl-devops-infrastructure-1/public/datahost-graphql@sha256:${var.digest}"
  }

  restart_policy = "Always"
}

resource "null_resource" "image_digest" {
  triggers = {
    digest = var.digest
  }
}

resource "google_service_account" "graphql_service_account" {
  account_id   = local.service_account_id
  display_name = "Service account to run the graphql server"
}

# service account needs to be able to read images from the swirrl-devops-infrastructure project
resource "google_project_iam_member" "image_creator_service_account_permissions" {
  project = "swirrl-devops-infrastructure-1"
  role = "roles/artifactregistry.reader"
  member = "serviceAccount:${google_service_account.graphql_service_account.email}"
}

resource "google_compute_instance" "datahost_instance" {
  name = "tf-datahost-graphql"
  machine_type = "e2-small"
  zone = local.gcloud_zone

  boot_disk {
    initialize_params {
      size = "10"
      image = module.gce_container_spec.source_image
    }
  }

  network_interface {
    network = "default"
    access_config {
      # ephemeral public ip
      # TODO: remove public ip and access only via load balancer
    }
  }

  tags = ["http-server", "https-server"]

  labels = {
    container-vm = module.gce_container_spec.vm_container_label
  }

  metadata = {
    gce-container-declaration = module.gce_container_spec.metadata_value
  }

  lifecycle {
    replace_triggered_by = [null_resource.image_digest]
  }

  service_account {
    email = google_service_account.graphql_service_account.email
    scopes = ["cloud-platform"]
  }
}

# load balancer
# backend
resource "google_compute_health_check" "lb_health_check" {
  name = local.health_check_name
  http_health_check {
    port = "80"
    request_path = "/ide"
  }
}

resource "google_compute_instance_group" "muttnik_instance_group" {
  name = local.instance_group_name
  zone = local.gcloud_zone

  instances = [google_compute_instance.datahost_instance.self_link]

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
    group = google_compute_instance_group.muttnik_instance_group.self_link
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

data "aws_route53_zone" "gss_zone" {
  name = "gss-data.org.uk"
}

resource "aws_route53_record" "graphql_record" {
  zone_id = data.aws_route53_zone.gss_zone.zone_id
  name = "graphql-prototype.gss-data.org.uk"
  type = "A"
  ttl = 300
  records = [google_compute_global_address.lb_address.address]
}