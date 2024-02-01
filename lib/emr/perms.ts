export const STUDIO_WORKSPACE_NETWORK_ACTIONS = [
  "ec2:CreateNetworkInterface",
  "ec2:CreateNetworkInterfacePermission",
  "ec2:DeleteNetworkInterface",
  "ec2:DeleteNetworkInterfacePermission",
  "ec2:DescribeNetworkInterfaces",
  "ec2:ModifyNetworkInterfaceAttribute",
  "ec2:AuthorizeSecurityGroupEgress",
  "ec2:AuthorizeSecurityGroupIngress",
  "ec2:CreateSecurityGroup",
  "ec2:DescribeSecurityGroups",
  "ec2:RevokeSecurityGroupEgress",
  "ec2:DescribeTags",
  "ec2:DescribeInstances",
  "ec2:DescribeSubnets",
  "ec2:DescribeVpcs",
  "elasticmapreduce:ListInstances",
  "elasticmapreduce:DescribeCluster",
  "elasticmapreduce:ListSteps",
];

export const STUDIO_READ_GIT_CREDENTIALS_ACTIONS = ["secretsmanager:GetSecretValue"];

export const STUDIO_WRITE_NETWORK_TAGS_ACTIONS = ["ec2:CreateTags"];

export const STUDIO_S3_NOTEBOOK_WRITE_ACTIONS = [
  "s3:PutObject",
  "s3:GetObject",
  "s3:GetEncryptionConfiguration",
  "s3:ListBucket",
  "s3:DeleteObject",
];

export const STUDIO_S3_ENCRYPTED_NOTEBOOK_WRITE_ACTIONS = [
  ...STUDIO_S3_NOTEBOOK_WRITE_ACTIONS,
  "kms:Decrypt",
  "kms:GenerateDataKey",
  "kms:ReEncryptFrom",
  "kms:ReEncryptTo",
  "kms:DescribeKey",
];

export const STUDIO_WORKSPACE_COLLABORATION_ACTIONS = [
  "iam:GetUser",
  "iam:GetRole",
  "iam:ListUsers",
  "iam:ListRoles",
  "sso:GetManagedApplicationInstance",
  "sso-directory:SearchUsers",
];
