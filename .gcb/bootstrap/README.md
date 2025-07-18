# Terraform configuration Bootstrap

We use terraform (https://terraform.io) to create and update the state of our
testing infrastructure. We describe the desired state of the infrastructure to
terraform using `.tf` scripts. By default, Terraform stores [state][tf-state]
locally, in a file called `terraform.tfstate`. This default configuration can
make Terraform usage difficult for teams. If multiple team members run Terraform
at the same time on different machine each state may have a different definition
of the desired and current state.

The recommended practice is to store terraform database in some remote storage,
shared by the whole team. Google Cloud Storage is one of the available options,
and one that works well for us.

This directory creates the initial Cloud Storage bucket to store the terraform's
for each sub-project. The scripts in this directory should be executed rarely,
ideally only once.

## Usage

Change your working directory, for example:

```shell
cd $HOME/google-cloud-rust/.gcb/bootstrap
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

## More Information

This directory follows the [Store Terraform state in a Cloud Storage bucket]
guide.

[store terraform state in a cloud storage bucket]: https://cloud.google.com/docs/terraform/resource-management/store-state
[tf-state]: https://www.terraform.io/docs/state/
