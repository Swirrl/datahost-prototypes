terraform {
  # S3 backend state configuration
  backend "s3" {
    bucket = "swirrl-terraform-states"
    key = "datahost/ons/ldapi.tfstate"
    region = "eu-west-2"

    dynamodb_table = "swirrl-terraform-locks"
    encrypt = true
  }
}

module "ons" {
  source = "../../modules/env"

  gcloud_project = "swirrl-ons-datahost"
  name = "ons"
  digest = var.digest

  gcloud_zone = "europe-west2-a"
  dns = {
    host = "ldapi-prototype"
    zone = "gss-data.org.uk"
  }
}