# Terraform

This directory contains Terraform modules for configuring a deployment of the GraphQL prototype. The prototype is
currently hosted within its own `swirrl-ons-datahost` gcloud project. The two modules configure aspects of the host
project (required services, firewall rules etc.) and the provisioning of a VM running the configured version of the
GraphQL application.

## Running Terraform

### Directly

The required version of Terraform is listed within the `.tool-versions` file. You can install this manually via the
[instructions on the downloads page](https://developer.hashicorp.com/terraform/downloads?product_intent=terraform) or
using [asdf](https://asdf-vm.com/)

```
asdf plugin-add terraform https://github.com/asdf-community/asdf-hashicorp.git
asdf install
```

### Via Docker

You can also run Terraform via the corresponding Docker container. The AWS and GCloud providers are configured through
the environment (see [below](#configuration), so these variables must be provided to the container. Similarly, the module root directory must also
be mapped into the container. For example, if your current directory is a module root directory, you can run `terraform init`
with:

```
docker run --rm --workdir /infra -v .:/infra \
           --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
           --env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
           --env AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN} \
           --env GOOGLE_OAUTH_ACCESS_TOKEN=${GOOGLE_OAUTH_ACCESS_TOKEN} \
           hashicorp/terraform:1.4.6 init 
```

## Configuration

### AWS

The AWS provider requires `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and optionally `AWS_SESSION_TOKEN` to be set in 
the environment. The corresponding user must have permission to read and write to the `swirrl-terraform-states` S3 bucket
where all Terraform state files are stored.

It is recommended you use [aws vault](https://github.com/99designs/aws-vault) to manage AWS credentials and generate
short-lived sessions.

### GCloud

It is recommended you configure the GCloud provider by generating a short-lived access token and exporting it via the
`GOOGLE_OAUTH_ACCESS_TOKEN` environment variable e.g.

    export GOOGLE_OAUTH_ACCESS_TOKEN=$(gcloud auth print-access-token)

## Secrets

The LDAPI service is protected by basic authentication. The username is `idp` and the password is configured for each environment
within a secret manager secret. The ldapi Terraform configuration only creates the secret and does not configure the secret value.
The name of the secret is defined as an output `basic_auth_password_secret_name` of the corresponding environment root module.
This output is displayed when running a `terraform apply`, and the corresponding secret should be configured manually via the CLI or web UI.
The LDAPI application always uses the latest version of this secret to configure basic authentication on startup.



