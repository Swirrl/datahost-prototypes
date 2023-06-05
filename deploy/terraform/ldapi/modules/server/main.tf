locals {
  gcloud_zone = "europe-west2-a"
  service_account_id = "${var.name}-account"
  data_disk_name = "${var.name}-data"
}

module "gce_container_spec" {
  source = "terraform-google-modules/container-vm/google"
  version = "~> 2.0"

  container = {
    image = "europe-west2-docker.pkg.dev/swirrl-devops-infrastructure-1/public/datahost-ld-openapi@sha256:${var.digest}"
    volumeMounts = [
      {
        mountPath = "/cache"
        name = "ldapi-data"
        readOnly = false
      }
    ]
  }

  volumes = [
    {
      name = "ldapi-data"
      gcePersistentDisk = {
        pdName = "data"
        fsType = "ext4"
      }
    }
  ]

  restart_policy = "Always"
}

resource "null_resource" "image_digest" {
  triggers = {
    digest = var.digest
  }
}

resource "google_service_account" "ldapi_service_account" {
  account_id   = local.service_account_id
  display_name = "Service account to run the ldapi server"
}

# service account needs to be able to read images from the swirrl-devops-infrastructure project
resource "google_project_iam_member" "ldapi_service_account_permissions" {
  project = "swirrl-devops-infrastructure-1"
  role = "roles/artifactregistry.reader"
  member = "serviceAccount:${google_service_account.ldapi_service_account.email}"
}

resource "google_compute_disk" "datahost_ldapi_data" {
  name = local.data_disk_name
  type = "pd-ssd"
  zone = var.zone
  size = 10
}

resource "google_compute_instance" "datahost_ldapi_instance" {
  name = var.name
  machine_type = "e2-small"
  zone = var.zone

  boot_disk {
    initialize_params {
      size = "10"
      image = module.gce_container_spec.source_image
    }
  }

  attached_disk {
    source = google_compute_disk.datahost_ldapi_data.self_link
    device_name = "data"
    mode = "READ_WRITE"
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
    email = google_service_account.ldapi_service_account.email
    scopes = ["cloud-platform"]
  }
}