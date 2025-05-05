# Terraform for Auth Integration Tests

This document assumes you are familiar with the
[Terraform set up for `rust-sdk-testing`](/.gcb/bootstrap/README.md).

The terraform configuration for auth is separate because:

- the resources belong to a different project (`rust-auth-testing` vs.
  `rust-sdk-testing`)
- accessing the different projects requires different permissions

## Usage

Change your working directory, for example:

```shell
cd $HOME/google-cloud-rust/src/auth/.gcb/bootstrap
```

Initialize terraform:

```shell
terraform init
```

Restore the current state. This may result in no action if you happen to have an
up-to-date state in your local files.

```shell
terraform plan -out /tmp/bootstrap.tplan
```

Execute the plan:

```shell
terraform apply /tmp/bootstrap.tplan
```

Make any changes to the configuration and commit them to git:

```shell
git commit -m"Cool changes" .
```

Prepare and execute a plan to update the bucket:

```shell
terraform plan -out /tmp/update.tplan
terraform apply /tmp/update.tplan
```
