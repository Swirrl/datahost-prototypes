terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.64.0"
    }

    aws = {
      source = "hashicorp/aws"
      version = "5.0.1"
    }
  }
}

provider "google" {
  project = var.gcloud_project
  region = "europe-west2"
}

provider "aws" {
  region = "eu-west-2"
}

module "server" {
  source = "../server"
  gcloud_project = var.gcloud_project
  digest = var.digest
  name = "${var.name}-ldapi"
  zone = var.gcloud_zone
}

module "load_balancer" {
  source = "../load_balancer"

  env = var.name
  dns = var.dns
  server_zone = module.server.zone
  server_self_link = module.server.self_link
}