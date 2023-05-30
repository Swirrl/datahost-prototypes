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
  service_account_id = "${local.name}-account"
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

resource "google_compute_address" "datahost_instance_address" {
  name = "datahost-address"
  address_type = "EXTERNAL"
}

data "aws_route53_zone" "gss_zone" {
  name = "gss-data.org.uk"
}

resource "aws_route53_record" "graphql_record" {
  zone_id = data.aws_route53_zone.gss_zone.zone_id
  name = "graphql-prototype.gss-data.org.uk"
  type = "A"
  ttl = 300
  records = [google_compute_address.datahost_instance_address.address]
}

resource "google_compute_instance" "datahost_instance" {
  name = "tf-datahost-graphql"
  machine_type = "e2-small"
  zone = "europe-west2-b"

  boot_disk {
    initialize_params {
      size = "10"
      image = module.gce_container_spec.source_image
    }
  }

  network_interface {
    network = "default"
    access_config {
      nat_ip = google_compute_address.datahost_instance_address.address
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