import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { aws_iam as iam } from "aws-cdk-lib";
import { aws_ec2 as ec2 } from "aws-cdk-lib";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { CfnStudio } from "aws-cdk-lib/aws-emr";
import { PermissionsStack } from "../PermissionsStack";

export interface EMRStudioStackProps extends cdk.StackProps {
  readonly vpc: ec2.IVpc;
  readonly bucket: s3.IBucket;
  readonly permissions: PermissionsStack;
  readonly emrRuntimeRole: iam.Role;
}

export class EMRStudioStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: EMRStudioStackProps) {
    super(scope, id, props);

    const emrStudioServiceRole = new iam.Role(this, "emr-studio-service-role", {
      assumedBy: new iam.ServicePrincipal("elasticmapreduce.amazonaws.com").withConditions({
        StringEquals: { "aws:SourceAccount": this.account },
        ArnLike: { "aws:SourceArn": `arn:aws:elasticmapreduce:${this.region}:${this.account}:*` },
      }),
      inlinePolicies: {
        emrStudioServicePolicy: new iam.PolicyDocument({
          statements: this._serviceRolePolicy().concat([
            new iam.PolicyStatement({
              sid: "AllowS3GetEncryption",
              effect: iam.Effect.ALLOW,
              actions: ["s3:GetEncryptionConfiguration"],
              resources: [props.bucket.bucketArn],
            }),
          ]),
        }),
      },
    });

    // This role needs access to read/write to our S3 bucket
    const s3Grant = props.bucket.grantReadWrite(emrStudioServiceRole, "emr/*");

    // Create a new "basic" user of EMR Studio
    // https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-studio-user-permissions.html#emr-studio-permissions-policies
    // const studioUser = new iam.User(this, "emr-studio-basic-user", {
    //   userName: "studio-user",
    //   password: cdk.SecretValue.unsafePlainText("This1IsATerriblePassword!"),
    // });
    // props.bucket.grantReadWrite(studioUser, "emr/*");

    // Create Engine and Workspace security groups for the Studio
    const engineSecurityGroup = new ec2.SecurityGroup(this, "EngineSecurityGroup", {
      vpc: props.vpc,
      description: "EMR Studio Engine",
      allowAllOutbound: true,
    });
    cdk.Tags.of(engineSecurityGroup).add("for-use-with-amazon-emr-managed-policies", "true");
    // The workspace security group requires explicit egress access to the engine security group.
    // For that reason, we disable the default allow all.
    const workspaceSecurityGroup = new ec2.SecurityGroup(this, "WorkspaceSecurityGroup", {
      vpc: props.vpc,
      description: "EMR Studio Engine",
      allowAllOutbound: false,
    });
    cdk.Tags.of(workspaceSecurityGroup).add("for-use-with-amazon-emr-managed-policies", "true");
    workspaceSecurityGroup.addEgressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(443), "Required for outbound git access");
    workspaceSecurityGroup.addEgressRule(
      engineSecurityGroup,
      ec2.Port.tcp(18888),
      "Allow outbound traffic from from notebook to cluster for port 18888"
    );
    engineSecurityGroup.addIngressRule(
      workspaceSecurityGroup,
      ec2.Port.tcp(18888),
      "Allow inbound traffic to from notebook to cluster for port 18888"
    );

    const emrStudio = new CfnStudio(this, "oda-poc-studio", {
      name: "ODA_POC_Studio",
      authMode: "IAM",
      vpcId: props.vpc.vpcId,
      subnetIds: props.vpc.selectSubnets({ subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS }).subnetIds,
      serviceRole: emrStudioServiceRole.roleArn,
      workspaceSecurityGroupId: workspaceSecurityGroup.securityGroupId,
      engineSecurityGroupId: engineSecurityGroup.securityGroupId,
      defaultS3Location: `s3://${props.bucket.bucketName}/emr/studio/`,
    });

    emrStudio.node.addDependency(s3Grant);

    // Allow access to the Studio for our demo user
    // CreateStudioPresignedUrl is necessary, the other two
    // are optional and allow the user to list studios in the console
    const emrStudioUserAccessPolicy = this._studioAccessPolicy(emrStudio, emrStudioServiceRole);
    const emrStudioRuntimeRolePolicy = this._runtimeRolePolicies(props.emrRuntimeRole);
    const basicUserPolicy = this._basicUserPolicy();

    const emrEC2ReadPolicy = this._emrClusterViewPolicy();

    // Grant access to view/open studio in the console
    emrStudioUserAccessPolicy.attachToUser(props.permissions.engineerUser);
    emrStudioUserAccessPolicy.attachToUser(props.permissions.analystUser);

    // Grant access to perform basic actions within EMR Studio
    basicUserPolicy.attachToUser(props.permissions.engineerUser);
    basicUserPolicy.attachToUser(props.permissions.analystUser);

    // The users also need access to the EMR Studio S3 location
    props.bucket.grantReadWrite(props.permissions.engineerUser, "emr/studio/*");
    props.bucket.grantReadWrite(props.permissions.analystUser, "emr/studio/*");

    // EMR Studio users need the ability to connect to EMR on EC2 with Runtime roles
    emrStudioRuntimeRolePolicy.attachToUser(props.permissions.engineerUser);
    emrStudioRuntimeRolePolicy.attachToUser(props.permissions.analystUser);

    // Finally users should have access to look at details of EMR clusters
    emrEC2ReadPolicy.attachToUser(props.permissions.engineerUser);
    emrEC2ReadPolicy.attachToUser(props.permissions.analystUser);
  }

  /**
   * Creates a new managed policy for users to be able to view clusters in the console.
   *
   *  @returns The IAM Managed Policy
   */
  _emrClusterViewPolicy(): iam.ManagedPolicy {
    return new iam.ManagedPolicy(this, "oda-poc-emr-studio-ec2-access", {
      document: new iam.PolicyDocument({
        statements: [
          new iam.PolicyStatement({
            sid: "AllowEMRReadOnlyActions",
            effect: iam.Effect.ALLOW,
            actions: [
              "elasticmapreduce:ListInstances",
              "elasticmapreduce:ListInstanceFleets",
              "elasticmapreduce:GetAutoTerminationPolicy",
              "elasticmapreduce:GetManagedScalingPolicy",
              "elasticmapreduce:DescribeInstanceTypes",
              "elasticmapreduce:DescribeSecurityConfiguration",
              "elasticmapreduce:addJobFlowSteps",
            ],
            resources: ["*"],
          }),
        ],
      }),
    });
  }

  /**
   * Creates a new managed policy for the provided EMR Studio and Service Role.
   *
   * @param studio
   * @param serviceRole
   * @returns The IAM Managed Policy
   */
  _studioAccessPolicy(studio: CfnStudio, serviceRole: iam.Role): iam.ManagedPolicy {
    return new iam.ManagedPolicy(this, "oda-poc-emr-studio-policy", {
      document: new iam.PolicyDocument({
        statements: [
          new iam.PolicyStatement({
            actions: [
              "elasticmapreduce:CreateStudioPresignedUrl",
              "elasticmapreduce:DescribeStudio",
              "elasticmapreduce:ListStudioSessionMappings",
            ],
            resources: [studio.attrArn],
          }),
          new iam.PolicyStatement({
            actions: ["elasticmapreduce:ListStudios"],
            resources: ["*"],
          }),
          new iam.PolicyStatement({
            actions: ["secretsmanager:TagResource"],
            resources: ["arn:aws:secretsmanager:*:*:secret:emr-studio-*"],
          }),
          new iam.PolicyStatement({
            actions: ["iam:PassRole"],
            resources: [serviceRole.roleArn],
          }),
        ],
      }),
    });
  }

  _serviceRolePolicy() {
    // Creates the set of policies necessary for EMR Studio
    // https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-studio-service-role.html
    return [
      new iam.PolicyStatement({
        sid: "AllowEMRReadOnlyActions",
        effect: iam.Effect.ALLOW,
        actions: ["elasticmapreduce:ListInstances", "elasticmapreduce:DescribeCluster", "elasticmapreduce:ListSteps"],
        resources: ["*"],
      }),
      new iam.PolicyStatement({
        sid: "AllowEC2ENIActionsWithEMRTags",
        effect: iam.Effect.ALLOW,
        actions: ["ec2:CreateNetworkInterfacePermission", "ec2:DeleteNetworkInterface"],
        resources: ["arn:aws:ec2:*:*:network-interface/*"],
        conditions: {
          StringEquals: {
            "aws:ResourceTag/for-use-with-amazon-emr-managed-policies": "true",
          },
        },
      }),
      new iam.PolicyStatement({
        sid: "AllowEC2ENIAttributeAction",
        effect: iam.Effect.ALLOW,
        actions: ["ec2:ModifyNetworkInterfaceAttribute"],
        resources: [
          "arn:aws:ec2:*:*:instance/*",
          "arn:aws:ec2:*:*:network-interface/*",
          "arn:aws:ec2:*:*:security-group/*",
        ],
      }),
      new iam.PolicyStatement({
        sid: "AllowEC2SecurityGroupActionsWithEMRTags",
        effect: iam.Effect.ALLOW,
        actions: [
          "ec2:AuthorizeSecurityGroupEgress",
          "ec2:AuthorizeSecurityGroupIngress",
          "ec2:RevokeSecurityGroupEgress",
          "ec2:RevokeSecurityGroupIngress",
          "ec2:DeleteNetworkInterfacePermission",
        ],
        resources: ["*"],
        conditions: {
          StringEquals: {
            "aws:ResourceTag/for-use-with-amazon-emr-managed-policies": "true",
          },
        },
      }),
      new iam.PolicyStatement({
        sid: "AllowDefaultEC2SecurityGroupsCreationWithEMRTags",
        effect: iam.Effect.ALLOW,
        actions: ["ec2:CreateSecurityGroup"],
        resources: ["arn:aws:ec2:*:*:security-group/*"],
        conditions: {
          StringEquals: {
            "aws:RequestTag/for-use-with-amazon-emr-managed-policies": "true",
          },
        },
      }),
      new iam.PolicyStatement({
        sid: "AllowDefaultEC2SecurityGroupsCreationInVPCWithEMRTags",
        effect: iam.Effect.ALLOW,
        actions: ["ec2:CreateSecurityGroup"],
        resources: ["arn:aws:ec2:*:*:vpc/*"],
        conditions: {
          StringEquals: {
            "aws:ResourceTag/for-use-with-amazon-emr-managed-policies": "true",
          },
        },
      }),
      new iam.PolicyStatement({
        sid: "AllowAddingEMRTagsDuringDefaultSecurityGroupCreation",
        effect: iam.Effect.ALLOW,
        actions: ["ec2:CreateTags"],
        resources: ["arn:aws:ec2:*:*:security-group/*"],
        conditions: {
          StringEquals: {
            "aws:RequestTag/for-use-with-amazon-emr-managed-policies": "true",
            "ec2:CreateAction": "CreateSecurityGroup",
          },
        },
      }),
      new iam.PolicyStatement({
        sid: "AllowEC2ENICreationWithEMRTags",
        effect: iam.Effect.ALLOW,
        actions: ["ec2:CreateNetworkInterface"],
        resources: ["arn:aws:ec2:*:*:network-interface/*"],
        conditions: {
          StringEquals: {
            "aws:RequestTag/for-use-with-amazon-emr-managed-policies": "true",
          },
        },
      }),
      new iam.PolicyStatement({
        sid: "AllowEC2ENICreationInSubnetAndSecurityGroupWithEMRTags",
        effect: iam.Effect.ALLOW,
        actions: ["ec2:CreateNetworkInterface"],
        resources: ["arn:aws:ec2:*:*:subnet/*", "arn:aws:ec2:*:*:security-group/*"],
        conditions: {
          StringEquals: {
            "aws:ResourceTag/for-use-with-amazon-emr-managed-policies": "true",
          },
        },
      }),
      new iam.PolicyStatement({
        sid: "AllowAddingTagsDuringEC2ENICreation",
        effect: iam.Effect.ALLOW,
        actions: ["ec2:CreateTags"],
        resources: ["arn:aws:ec2:*:*:network-interface/*"],
        conditions: {
          StringEquals: {
            "ec2:CreateAction": "CreateNetworkInterface",
          },
        },
      }),
      new iam.PolicyStatement({
        sid: "AllowEC2ReadOnlyActions",
        effect: iam.Effect.ALLOW,
        actions: [
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DescribeTags",
          "ec2:DescribeInstances",
          "ec2:DescribeSubnets",
          "ec2:DescribeVpcs",
        ],
        resources: ["*"],
      }),
      new iam.PolicyStatement({
        sid: "AllowSecretsManagerReadOnlyActionsWithEMRTags",
        effect: iam.Effect.ALLOW,
        actions: ["secretsmanager:GetSecretValue"],
        resources: ["arn:aws:secretsmanager:*:*:secret:*"],
        conditions: {
          StringEquals: {
            "aws:ResourceTag/for-use-with-amazon-emr-managed-policies": "true",
          },
        },
      }),
      new iam.PolicyStatement({
        sid: "AllowWorkspaceCollaboration",
        effect: iam.Effect.ALLOW,
        actions: [
          "iam:GetUser",
          "iam:GetRole",
          "iam:ListUsers",
          "iam:ListRoles",
          "sso:GetManagedApplicationInstance",
          "sso-directory:SearchUsers",
        ],
        resources: ["*"],
      }),
    ];
  }

  /**
   * Policy statements to allow a user to use runtime roles with Studio
   * https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-studio-runtime.html
   *
   * @param role The role to grant access to
   */
  _runtimeRolePolicies(role: iam.Role): iam.ManagedPolicy {
    return new iam.ManagedPolicy(this, "oda-poc-emr-runtimerole-policy", {
      document: new iam.PolicyDocument({
        statements: [
          new iam.PolicyStatement({
            sid: "AllowSpecificExecRoleArn",
            actions: ["elasticmapreduce:GetClusterSessionCredentials"],
            resources: ["*"],
            conditions: { StringEquals: { "elasticmapreduce:ExecutionRoleArn": [role.roleArn] } },
          }),
        ],
      }),
    });
  }

  /**
   * Managed policy for "basic" user functionality inside studio
   *
   * @returns IAM Managed Policy
   */
  _basicUserPolicy(): iam.ManagedPolicy {
    return new iam.ManagedPolicy(this, "studio-basic-user-policy", {
      document: new iam.PolicyDocument({
        statements: this._commonUserPolicies(),
      }),
    });
  }

  _commonUserPolicies(): iam.PolicyStatement[] {
    return [
      new iam.PolicyStatement({
        sid: "AccessWorkspaces",
        actions: [
          "elasticmapreduce:DescribeEditor",
          "elasticmapreduce:ListEditors",
          "elasticmapreduce:StartEditor",
          "elasticmapreduce:StopEditor",
          "elasticmapreduce:OpenEditorInConsole",
        ],
        resources: ["*"],
      }),
      new iam.PolicyStatement({
        sid: "CreateAndDeleteWorkspaces",
        actions: [
          "elasticmapreduce:CreateEditor",
          "elasticmapreduce:DescribeEditor",
          "elasticmapreduce:ListEditors",
          "elasticmapreduce:DeleteEditor",
        ],
        resources: ["*"],
      }),
      new iam.PolicyStatement({
        sid: "AttachAndDetachClusters",
        actions: [
          "elasticmapreduce:AttachEditor",
          "elasticmapreduce:DetachEditor",
          "elasticmapreduce:ListClusters",
          "elasticmapreduce:DescribeCluster",
          "elasticmapreduce:ListInstanceGroups",
          "elasticmapreduce:ListBootstrapActions",
        ],
        resources: ["*"],
      }),
      new iam.PolicyStatement({
        sid: "AccessEMRPersistentUIs",
        actions: [
          "elasticmapreduce:CreatePersistentAppUI",
          "elasticmapreduce:DescribePersistentAppUI",
          "elasticmapreduce:GetPersistentAppUIPresignedURL",
          "elasticmapreduce:GetOnClusterAppUIPresignedURL",
          "elasticmapreduce:ListClusters",
          "elasticmapreduce:ListSteps",
          "elasticmapreduce:DescribeCluster",
        ],
        resources: ["*"],
      }),
      new iam.PolicyStatement({
        sid: "CreateAndDeleteGitRepositories",
        actions: [
          "elasticmapreduce:CreateRepository",
          "elasticmapreduce:DeleteRepository",
          "elasticmapreduce:ListRepositories",
          "elasticmapreduce:DescribeRepository",
          "secretsmanager:CreateSecret",
          "secretsmanager:ListSecrets",
          "secretsmanager:TagResource",
        ],
        resources: ["*"],
      }),
      new iam.PolicyStatement({
        sid: "LinkAndUnlinkGitRepositories",
        actions: [
          "elasticmapreduce:LinkRepository",
          "elasticmapreduce:UnlinkRepository",
          "elasticmapreduce:ListRepositories",
          "elasticmapreduce:DescribeRepository",
        ],
        resources: ["*"],
      }),
      new iam.PolicyStatement({
        sid: "EC2ReadAccess",
        actions: ["ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups"],
        resources: ["*"],
      }),
      new iam.PolicyStatement({
        sid: "ListIAMRoles",
        actions: ["iam:ListRoles"],
        resources: ["*"],
      }),
    ];
  }

  _commonUserPoliciesButNotActually(): iam.PolicyStatement[] {
    return [
      new iam.PolicyStatement({
        sid: "AllowEMRBasicActions",
        actions: [
          "elasticmapreduce:CreateEditor",
          "elasticmapreduce:DescribeEditor",
          "elasticmapreduce:ListEditors",
          "elasticmapreduce:StartEditor",
          "elasticmapreduce:StopEditor",
          "elasticmapreduce:DeleteEditor",
          "elasticmapreduce:OpenEditorInConsole",
          "elasticmapreduce:AttachEditor",
          "elasticmapreduce:DetachEditor",
          "elasticmapreduce:CreateRepository",
          "elasticmapreduce:DescribeRepository",
          "elasticmapreduce:DeleteRepository",
          "elasticmapreduce:ListRepositories",
          "elasticmapreduce:LinkRepository",
          "elasticmapreduce:UnlinkRepository",
          "elasticmapreduce:DescribeCluster",
          "elasticmapreduce:ListInstanceGroups",
          "elasticmapreduce:ListBootstrapActions",
          "elasticmapreduce:ListClusters",
          "elasticmapreduce:ListSteps",
          "elasticmapreduce:CreatePersistentAppUI",
          "elasticmapreduce:DescribePersistentAppUI",
          "elasticmapreduce:GetPersistentAppUIPresignedURL",
          "elasticmapreduce:GetOnClusterAppUIPresignedURL",
        ],
        resources: ["*"],
      }),
      new iam.PolicyStatement({
        sid: "AllowEMRContainersBasicActions",
        resources: ["*"],
        actions: [
          "emr-containers:DescribeVirtualCluster",
          "emr-containers:ListVirtualClusters",
          "emr-containers:DescribeManagedEndpoint",
          "emr-containers:ListManagedEndpoints",
          "emr-containers:CreateAccessTokenForManagedEndpoint",
          "emr-containers:DescribeJobRun",
          "emr-containers:ListJobRuns",
        ],
      }),
      new iam.PolicyStatement({
        sid: "AllowSecretManagerListSecrets",
        resources: ["*"],
        actions: ["secretsmanager:ListSecrets"],
      }),
      new iam.PolicyStatement({
        sid: "AllowSecretCreationWithEMRTagsAndEMRStudioPrefix",
        resources: ["arn:aws:secretsmanager:*:*:secret:emr-studio-*"],
        actions: ["secretsmanager:CreateSecret"],
        conditions: {
          StringEquals: {
            "aws:RequestTag/for-use-with-amazon-emr-managed-policies": "true",
          },
        },
      }),
      new iam.PolicyStatement({
        sid: "AllowAddingTagsOnSecretsWithEMRStudioPrefix",
        resources: ["arn:aws:secretsmanager:*:*:secret:emr-studio-*"],
        actions: ["secretsmanager:TagResource"],
      }),
      new iam.PolicyStatement({
        sid: "AllowS3ListAndLocationPermissions",
        actions: ["s3:ListAllMyBuckets", "s3:ListBucket", "s3:GetBucketLocation"],
        resources: ["arn:aws:s3:::*"],
      }),
      // Fix this - we put logs in a different place
      new iam.PolicyStatement({
        sid: "AllowS3ReadOnlyAccessToLogs",
        actions: ["s3:GetObject"],
        resources: [`arn:aws:s3:::aws-logs-${this.account}-${this.region}/elasticmapreduce/*`],
      }),
    ];
  }
}
