locals {
  gcloud_zone = "europe-west2-a"
  service_account_id = "${var.name}-account"
}

module "gce_container_spec" {
  source = "terraform-google-modules/container-vm/google"
  version = "~> 2.0"

  container = {
    image = "europe-west2-docker.pkg.dev/swirrl-devops-infrastructure-1/public/datahost-graphql@sha256:${var.digest}"
  }

  restart_policy = "Always"
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
  name = "${var.name}-${substr(var.digest, 0, 8)}"
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

  service_account {
    email = google_service_account.graphql_service_account.email
    scopes = ["cloud-platform"]
  }
}