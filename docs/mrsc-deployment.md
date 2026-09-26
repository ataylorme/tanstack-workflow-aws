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

Use least privilege: scope table and index actions to the chosen table and its children in the participating Regions (`us-west-2`, `us-east-1`, `us-east-2`). Include replica/witness creation, description, update and deletion permissions appropriate to the template. Consult AWS's [GlobalTable permission requirements](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-dynamodb-globaltable.html) for the complete action list and requirements for any enabled features. Actions that do not support resource scoping need separate statements; do not assume every listed action accepts a table ARN. If the DynamoDB replication service-linked role does not exist, allow its creation with `iam:CreateServiceLinkedRole`, restricted to the DynamoDB replication service. A runtime store or sweeper role is not a provisioning role.

**Security:** once associated, CloudFormation continues using the service role for stack operations; it cannot simply be removed. Other principals authorized to operate the stack can exercise its permissions even without their own `iam:PassRole`. Restrict both stack access and role permissions. See AWS's [service-role guidance](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-iam-servicerole.html).

In [issue #3](https://github.com/ataylorme/tanstack-workflow-aws/issues/3), an initial deployment using temporary IAM-user credentials from AWS CLI `aws login` created a regional table but failed while adding the MRSC topology with `UnrecognizedClientException`. STS and direct DynamoDB calls still worked. Retrying the same table properties with an explicit execution role succeeded in that environment. This is a deployment compatibility observation, **not proof of a universal `aws login` incompatibility or its underlying cause**, and not a demonstrated runtime/store bug.

## Verify provisioning

Wait for the stack to finish successfully, then inspect the table in both replica Regions:

```sh
aws cloudformation describe-stacks --region us-west-2 --stack-name workflow-table
aws dynamodb describe-table --region us-west-2 --table-name tanstack-workflow-aws
aws dynamodb describe-table --region us-east-1 --table-name tanstack-workflow-aws
```

Confirm `MultiRegionConsistency` is `STRONG`, both replica tables are `ACTIVE`, the east replica is `ACTIVE` in the west description, and `GlobalTableWitnesses` reports the `us-east-2` witness as `ACTIVE`. A successful local `DescribeTable` alone does not establish that the complete MRSC topology exists. Do not enable application writers until provisioning is verified.

## Partial creation and a retained table

The template uses `DeletionPolicy: Retain` and `UpdateReplacePolicy: Retain`. A failed creation/rollback can therefore leave a regional-only table behind. Recreating a stack with the same name does not automatically adopt that retained table, and adding `--role-arn` does not resolve an existing-table name collision.

1. Record stack events, physical resource IDs, account, Regions and table descriptions before changing anything. Check caller identity with `aws sts get-caller-identity`; inspect the original failure and role permissions rather than assuming expired caller credentials explain it.
2. Establish ownership and usage. **Never delete an existing application table, a table containing data, or a table of uncertain provenance as this workaround.** `DescribeTable.ItemCount` is approximate and cannot prove emptiness. Stop all writers and use a fully paginated, strongly consistent base-table scan in each existing replica to check for items; if ownership, inactivity or emptiness cannot be established, preserve the resource and seek an operator-reviewed recovery/import or migration plan.
3. Prefer preserving the retained resource and retrying this fresh-table example with a new, isolated table name and stack name, updating the role's resource scope accordingly. Review cost and cleanup responsibilities for the retained table. Do not point the application at the new table until the intended data and topology are verified.
4. Only for a confirmed empty, disposable table belonging exclusively to the failed test deployment, an authorized operator may remove the failed stack and retained test resource before retrying with the execution role. Stack deletion does not itself remove a retained table. Inspect for partially created replicas/witnesses and use a topology-aware cleanup plan; do not automate deletion based on a failed stack status or table name alone.

This guide provides no destructive cleanup script and does not perform a migration of an existing table. Validate the execution role and recovery procedure in a disposable AWS environment before relying on them in production.
