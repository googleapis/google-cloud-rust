# Selected googleapis protos to test the generator

This directory contains a small subset of the [googleapis] protos for use in
testing the generator.

To update:

```shell
git -C $HOME clone https://github.com/googleapis/googleapis
git -C $HOME/googleapis checkout 2d08f07eab9bbe8300cd20b871d0811bbb693fab
cp $HOME/googleapis/google/api/annotations.proto    ./google/api
cp $HOME/googleapis/google/api/client.proto         ./google/api
cp $HOME/googleapis/google/api/field_behavior.proto ./google/api
cp $HOME/googleapis/google/api/http.proto           ./google/api
cp $HOME/googleapis/google/api/launch_stage.proto   ./google/api
cp $HOME/googleapis/google/api/resource.proto       ./google/api
cp $HOME/googleapis/google/type/expr.proto         ./google/type
cp $HOME/googleapis/google/iam/v1/iam_policy.proto ./google/iam/v1
cp $HOME/googleapis/google/iam/v1/policy.proto     ./google/iam/v1
cp $HOME/googleapis/google/iam/v1/options.proto    ./google/iam/v1
cp $HOME/googleapis/google/cloud/secretmanager/v1/resources.proto        ./google/cloud/secretmanager/v1
cp $HOME/googleapis/google/cloud/secretmanager/v1/secretmanager_v1.yaml  ./google/cloud/secretmanager/v1
cp $HOME/googleapis/google/cloud/secretmanager/v1/service.proto          ./google/cloud/secretmanager/v1
cp $HOME/googleapis/google/cloud/sql/v1/cloud_sql_backup_runs.proto          ./google/cloud/sql/v1
cp $HOME/googleapis/google/cloud/sql/v1/cloud_sql_connect.proto          ./google/cloud/sql/v1
cp $HOME/googleapis/google/cloud/sql/v1/cloud_sql_databases.proto          ./google/cloud/sql/v1
cp $HOME/googleapis/google/cloud/sql/v1/cloud_sql_flags.proto          ./google/cloud/sql/v1
cp $HOME/googleapis/google/cloud/sql/v1/cloud_sql_instances.proto          ./google/cloud/sql/v1
cp $HOME/googleapis/google/cloud/sql/v1/cloud_sql_operations.proto          ./google/cloud/sql/v1
cp $HOME/googleapis/google/cloud/sql/v1/cloud_sql_ssl_certs.proto          ./google/cloud/sql/v1
cp $HOME/googleapis/google/cloud/sql/v1/cloud_sql_tiers.proto          ./google/cloud/sql/v1
cp $HOME/googleapis/google/cloud/sql/v1/cloud_sql_users.proto          ./google/cloud/sql/v1
cp $HOME/googleapis/google/cloud/sql/v1/sqladmin_v1.yaml          ./google/cloud/sql/v1
cp $HOME/googleapis/google/cloud/sql/v1/cloud_sql_resources.proto          ./google/cloud/sql/v1
```
