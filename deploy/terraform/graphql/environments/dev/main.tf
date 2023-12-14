terraform {
  # S3 backend state configuration
  backend "s3" {
    bucket = "swirrl-terraform-states"
    key = "datahost/dev/graphql.tfstate"
    region = "eu-west-2"

    dynamodb_table = "swirrl-terraform-locks"
    encrypt = true
  }
}

module "dev" {
  source = "../../modules/env"

  name = "dev"
  gcloud_project = "swirrl-ons-datahost"
  digest = var.digest
  gcloud_zone = "europe-west2-a"
  dns = {
    host = "graphql-dev"
    zone = "gss-data.org.uk"
  }
}