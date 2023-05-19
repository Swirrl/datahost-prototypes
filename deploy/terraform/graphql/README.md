# GraphQL Terraform module

This directory contains a module defining the configuration for a VM running a version of the GraphQL application.
The VM runs under a service account with permission to read images published to the 

## Configuration

The module must be configured with the SHA256 digest of the image to run. This must identify an image published to the
`europe-west2-docker.pkg.dev/swirrl-devops-infrastructure-1/public/datahost-graphql` repository. The digest is specified
as a variable to the `terraform plan` command e.g.

    terraform plan -var digest=4c0c40fba6a658da6769a07a1370655b7c1e2aac03c0f91d89d6b8d32103a80b -out update.tfplan

## Updates

The host VM must be re-created if the configured Docker image changes. This will happen automatically when running `apply` e.g.

    terraform apply update.tfplan