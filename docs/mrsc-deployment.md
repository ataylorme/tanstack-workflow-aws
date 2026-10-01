# MRSC provisioning with a CloudFormation service role

Deploy [`global-table.yaml`](../cloudformation/global-table.yaml) once, from `us-west-2`. It provisions a fresh table with west/east replicas and an Ohio witness. Do not deploy another stack to manage the same table in another Region.

## Explicit execution-role option

Use an existing, administrator-provisioned CloudFormation execution role instead of relying on CloudFormation's default temporary session derived from caller credentials:

```sh
# Set this to the ARN of your approved execution role before running.
: "${CLOUDFORMATION_ROLE_ARN:?Set the CloudFormation execution role ARN}"
aws cloudformation deploy \
  --region us-west-2 \
  --stack-name workflow-table \
  --template-file cloudformation/global-table.yaml \
  --parameter-overrides TableName=tanstack-workflow-aws \
  --role-arn "$CLOUDFORMATION_ROLE_ARN"
```

The role must trust `cloudformation.amazonaws.com`. The deploying caller needs `iam:PassRole` for that role as well as the required CloudFormation permissions. Keep the role and its permissions available until asynchronous replica operations finish, and for future stack updates and deletion.

Use least privilege: scope table and index actions to the chosen table and its children in the participating Regions (`us-west-2`, `us-east-1`, `us-east-2`). Include replica/witness creation, description, update and deletion permissions appropriate to the template. Consult AWS's [GlobalTable permission requirements](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-dynamodb-globaltable.html) for the complete action list and requirements for any enabled features. Actions that do not support resource scoping need separate statements; do not assume every listed action accepts a table ARN. If the DynamoDB replication service-linked role does not exist, allow its creation with `iam:CreateServiceLinkedRole`, restricted to the DynamoDB replication service. A runtime store or worker role is not a provisioning role.

**Security:** once associated, CloudFormation continues using the service role for stack operations; it cannot simply be removed. Other principals authorized to operate the stack can exercise its permissions even without their own `iam:PassRole`. Restrict both stack access and role permissions. See AWS's [service-role guidance](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-iam-servicerole.html).

## Verify provisioning

Wait for the stack to finish successfully, then inspect the table in both replica Regions:

```sh
aws cloudformation describe-stacks --region us-west-2 --stack-name workflow-table
aws dynamodb describe-table --region us-west-2 --table-name tanstack-workflow-aws
aws dynamodb describe-table --region us-east-1 --table-name tanstack-workflow-aws
```

Confirm `MultiRegionConsistency` is `STRONG`, both replica tables are `ACTIVE`, the east replica is `ACTIVE` in the west description, and `GlobalTableWitnesses` reports the `us-east-2` witness as `ACTIVE`. A successful local `DescribeTable` alone does not establish that the complete MRSC topology exists. Do not enable application writers until provisioning is verified.

## Retained resources

The table has `DeletionPolicy: Retain` and `UpdateReplacePolicy: Retain`. Failed provisioning can leave retained resources. Inspect stack events and regional table descriptions, preserve any resource containing application data, and use a distinct isolated table name for a disposable deployment. Stack deletion does not delete the retained table.

After topology verification, confirm `StreamViewType=NEW_AND_OLD_IMAGES` in each replica and deploy one [workers stack](workflow-wakeups.md) per replica. Only the unified router attaches to each stream. Runtime roles need data-plane permissions; they do not need provisioning permissions.
