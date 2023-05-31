terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.50.0"
    }
  }

  # S3 backend state configuration
  backend "s3" {
    bucket = "swirrl-terraform-states"
    key = "datahost/gcloud/swirrl-ons-datahost.tfstate"
    region = "eu-west-2"

    dynamodb_table = "swirrl-terraform-locks"
    encrypt = true
  }
}

# config for GCP provider
provider "google" {
  project = local.project
  region = "europe-west2"
}

locals {
  project = "swirrl-ons-datahost"
  services = [
    "compute.googleapis.com",
    "iamcredentials.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com"
  ]
  ci_service_account_roles = [
    "roles/compute.instanceAdmin.v1",
    "roles/iam.serviceAccountUser",
    "roles/compute.loadBalancerAdmin"
  ]
  swirrl_circleci_organisation_id = "294dc3ff-773f-44a8-820e-f12f3ba2e1ac"
}

# services required within the project
resource "google_project_service" "services" {
  count = length(local.services)
  service = local.services[count.index]
}

## firewall rules

# Allow incoming SSH for VMs with ssh-server tag
resource "google_compute_firewall" "allow_ssh" {
  name = "allow-ssh"
  network = "default"
  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags = ["ssh-server"]
}

resource "google_compute_firewall" "allow_https" {
  name = "allow-https"
  network = "default"
  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports = ["443"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags = ["https-server"]
}

resource "google_compute_firewall" "allow_http" {
  name = "allow-http"
  network = "default"
  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports = ["80"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags = ["http-server"]
}

# Service account for circle ci
resource "google_service_account" "ci_account" {
  account_id   = "circleci"
  display_name = "Service account impersonated by circleci jobs"
}

resource "google_project_iam_member" "ci_service_account_permissions" {
  project = local.project
  count = length(local.ci_service_account_roles)
  role = local.ci_service_account_roles[count.index]
  member = "serviceAccount:${google_service_account.ci_account.email}"
}

# CI account needs to be able to make IAM changes within the swirrl-devops-infrastructure-1 project
resource "google_project_iam_member" "ci_devops_infrastructure_iam" {
  project = "swirrl-devops-infrastructure-1"
  role = "roles/iam.securityAdmin" // TODO: is there a role with fewer permissions?!
  member = "serviceAccount:${google_service_account.ci_account.email}"
}

# configure workload identity pool for CircleCI
# this allows CircleCI access tokens to be exchanged for a gcloud token
# see https://circleci.com/blog/openid-connect-identity-tokens/
resource "google_iam_workload_identity_pool" "circleci" {
  workload_identity_pool_id = "circleci-oidc-pool"
  display_name = "CirclCI Workload Identity Pool"
  description = "Workload Identity Pool used to create tokens from CircleCI OIDC tokens"
}

resource "google_iam_workload_identity_pool_provider" "circleci_provider" {
  workload_identity_pool_id = google_iam_workload_identity_pool.circleci.workload_identity_pool_id
  workload_identity_pool_provider_id = "circlci-oidc-provider"
  display_name = "CircleCI OIDC Provider"
  description = "Identity provider for CircleCI OIDC tokens"
  attribute_condition = "attribute.org_id=='${local.swirrl_circleci_organisation_id}'"
  attribute_mapping = {
    "google.subject" = "assertion.sub"
    "attribute.org_id" = "assertion.aud"
  }
  oidc {
    allowed_audiences = [local.swirrl_circleci_organisation_id]
    issuer_uri = "https://oidc.circleci.com/org/${local.swirrl_circleci_organisation_id}"
  }
}

# required to get project number for the devops infrastructure project
data "google_project" "deployment_project" {
  project_id = local.project
}

# map CircleCI tokens to service account
# allow any tokens with the Swirrl organisation id to impersonate the service account
# see https://cloud.google.com/iam/docs/workload-identity-federation
resource "google_service_account_iam_member" "circleci_binding" {
  service_account_id = google_service_account.ci_account.name
  role = "roles/iam.workloadIdentityUser"
  member = "principalSet://iam.googleapis.com/projects/${data.google_project.deployment_project.number}/locations/global/workloadIdentityPools/${google_iam_workload_identity_pool.circleci.workload_identity_pool_id}/attribute.org_id/${local.swirrl_circleci_organisation_id}"
}