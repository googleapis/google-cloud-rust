#!/bin/bash
#
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Usage: terraform.sh -d <terraform_dir>

set -euo pipefail

DIR=""

while getopts "d:" opt; do
  case ${opt} in
    d)
      DIR=${OPTARG}
      ;;
    ?)
      echo "Invalid option: -${OPTARG}."
      exit 1
      ;;
  esac
done

echo "Running terraform on ${DIR}"
cd ${DIR}
terraform init
terraform plan -out /tmp/bootstrap.tplan
terraform apply /tmp/bootstrap.tplan