import { IResource, Resource, Stack, Tags } from "aws-cdk-lib";
import { aws_ec2 as ec2 } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { ISecurityGroup } from "aws-cdk-lib/aws-ec2";
import { CfnStudio } from "aws-cdk-lib/aws-emr";
import { Construct } from "constructs";
import { Cluster } from "./cluster";
import { Application } from "./serverless";

/**
 * Studio S3 prefix constant
 */
const NOTEBOOK_STORAGE_PREFIX: string = "emr/studio/";

export interface IStudio extends IResource, iam.IGrantable {
  /**
   * Manage the allowed network connections for the cluster with Security Groups.
   */
  readonly connections: ec2.Connections;

  /**
   * Grant access to use EMR Studio in the console.
   *
   * @param grantee
   */

  grantConsoleAccess(grantee: iam.IPrincipal): iam.Grant;
}

export interface StudioProps {
  /**
   * VPC to launch the cluster in.
   *
   */
  readonly vpc: ec2.IVpc;

  /**
   * S3 bucket for storage of notebooks
   *
   * @default - A bucket will automatically be created, accessed via the `bucket` property
   */
  readonly bucket?: s3.IBucket;

  /**
   * Prefix where notebooks are saved in the S3 bucket
   *
   * @default - emr/studio/
   */
  readonly bucketNotebookPrefix?: string;

  /**
   * An IAM role used by EMR Studio when ...
   *
   * The role must be assumable by the service principal `elasticmapreduce.amazonaws.com`:
   * and must have the set of permissions required to interact with other AWS services.
   * More details: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-studio-service-role.html
   *
   * @example
   * const role = new iam.Role(this, 'MyRole', {
   *   assumedBy: new iam.ServicePrincipal('elasticmapreduce.amazonaws.com')
   * });
   *
   * @default - A role will automatically be created, it can be accessed via the `serviceRole` property
   */
  readonly serviceRole?: iam.IRole;

  /**
   * Security Group to allow communication to the EMR engine
   *
   * More details: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-studio-security-groups.html
   *
   * @default - create new security group
   */
  readonly engineSecurityGroup?: ISecurityGroup;

  /**
   * Security Group to allow outbound access from Workspaces to the internet and git repositories
   *
   * More details: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-studio-security-groups.html
   *
   * @default - create new security group
   */
  readonly workspaceSecurityGroup?: ISecurityGroup;

  /**
   * Whether to enable workspace collaboration or not.
   *
   * @default - False
   */
  readonly workspaceCollaborationEnabled?: boolean;
}

export class Studio extends Resource implements IStudio {
  grantPrincipal: iam.IPrincipal;
  public readonly connections: ec2.Connections;

  /**
   * The underlying EMR Studio
   */
  public readonly studio: CfnStudio;
  /**
   * The access URL of the EMR Studio
   */
  public readonly studioUrl: string;

  /**
   * The S3 bucket used for notebook storage.
   */
  public readonly bucket: s3.IBucket;

  /**
   * The bucket prefix used for notebook storage.
   */
  public readonly bucketNotebookPrefix: string;

  /**
   * Whether workspace collaboration is enabled or not.
   */
  public readonly workspaceCollaborationEnabled: boolean;

  /**
   * The IAM role used by EMR for service-level tasks.
   */
  public readonly serviceRole: iam.IRole;

  /**
   * Engine and Workspace security groups
   */
  public readonly engineSecurityGroup: ISecurityGroup;
  public readonly workspaceSecurityGroup: ISecurityGroup;

  /**
   * The minimal policy needed to use notebooks in EMR Studio
   */
  public readonly notebookUserPolicy: iam.ManagedPolicy;

  /**
   * The minimal policy needed to access the EMR Studio console
   */
  public readonly consoleAccessPolicy: iam.ManagedPolicy;

  constructor(scope: Construct, id: string, props: StudioProps) {
    super(scope, id);

    this.bucketNotebookPrefix =
      props.bucketNotebookPrefix ?? NOTEBOOK_STORAGE_PREFIX;

    this.workspaceCollaborationEnabled =
      props.workspaceCollaborationEnabled ?? false;
    this.bucket =
      props.bucket ??
      new s3.Bucket(this, "Bucket", {
        versioned: true,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      });
    this.serviceRole = props.serviceRole ?? this.createServiceRole();
    const s3Grant = this.bucket.grantReadWrite(
      this.serviceRole,
      `${this.bucketNotebookPrefix}*`
    );

    // Create relevant security groups if needed
    this.engineSecurityGroup =
      props.engineSecurityGroup ?? this.createEngineSecurityGroup(props.vpc);
    this.workspaceSecurityGroup =
      props.workspaceSecurityGroup ??
      this.createWorkspaceSecurityGroup(props.vpc);

    // Even if security groups are passed in, add ingress/egress
    this.workspaceSecurityGroup.addEgressRule(
      this.engineSecurityGroup,
      ec2.Port.tcp(18888),
      "Allow outbound traffic from from notebook to cluster for port 18888"
    );
    this.engineSecurityGroup.addIngressRule(
      this.workspaceSecurityGroup,
      ec2.Port.tcp(18888),
      "Allow inbound traffic to from notebook to cluster for port 18888"
    );

    // Create the Studio
    this.studio = new CfnStudio(this, "Studio", {
      name: "ODA_POC_Studio",
      authMode: "IAM",
      vpcId: props.vpc.vpcId,
      subnetIds: props.vpc.selectSubnets({
        subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
      }).subnetIds,
      serviceRole: this.serviceRole.roleArn,
      workspaceSecurityGroupId: this.workspaceSecurityGroup.securityGroupId,
      engineSecurityGroupId: this.engineSecurityGroup.securityGroupId,
      defaultS3Location: `s3://${this.bucket.bucketName}/${this.bucketNotebookPrefix}`,
    });

    // The service role requires access to read/write to S3 prior to creating the Studio
    this.studio.node.addDependency(s3Grant);
    this.studioUrl = this.studio.attrUrl;

    // Create a basic user policy that can be used to allow notebook users
    this.notebookUserPolicy = this.generateUserPolicy();
    this.consoleAccessPolicy = this.generateConsolePolicy(
      this.studio,
      this.serviceRole
    );
  }
  generateConsolePolicy(
    studio: CfnStudio,
    serviceRole: iam.IRole
  ): iam.ManagedPolicy {
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

  /**
   * Allows a user to access EMR Studio and the EMR console by adding
   * pre-created managed policies to the user.
   *
   * If an EMR resource is passed in *and* the resource is using runtime roles,
   * we add (or create?) the proper AssumeRole mappings.
   *
   * @param user
   */

  public addUser(user: iam.User, emr?: Cluster|Application) {
    // Note that this.notebookUserPolicy.attachToUser results in a cyclic dependency,
    // while user.addManagedPolicy does not. 👀
    user.addManagedPolicy(this.notebookUserPolicy);
    user.addManagedPolicy(this.consoleAccessPolicy);
    this.bucket.grantReadWrite(user, `${this.bucketNotebookPrefix}*`);

    if (emr !== undefined) {
      user.addManagedPolicy(emr.getStudioAccessPolicy());
    }
  }

  public grantConsoleAccess(grantee: iam.IPrincipal): iam.Grant {
    const g1 = iam.Grant.addToPrincipal({
      grantee,
      actions: [
        "elasticmapreduce:CreateStudioPresignedUrl",
        "elasticmapreduce:DescribeStudio",
        "elasticmapreduce:ListStudioSessionMappings",
      ],
      resourceArns: [this.studio.attrArn],
    });

    const g2 = iam.Grant.addToPrincipal({
      grantee,
      actions: ["elasticmapreduce:ListStudios"],
      resourceArns: ["*"],
    });

    const g3 = iam.Grant.addToPrincipal({
      grantee,
      actions: ["secretsmanager:TagResource"],
      resourceArns: ["arn:aws:secretsmanager:*:*:secret:emr-studio-*"],
    });

    const g4 = iam.Grant.addToPrincipal({
      grantee,
      actions: ["iam:PassRole"],
      resourceArns: [this.serviceRole.roleArn],
    });

    return g1.combine(g2).combine(g3).combine(g4);
  }

  public grantBasicUserAccess(principal: iam.IPrincipal) {}

  private createServiceRole(): iam.Role {
    const serviceRole = new iam.Role(this, "ServiceRole", {
      assumedBy: new iam.ServicePrincipal(
        "elasticmapreduce.amazonaws.com"
      ).withConditions({
        StringEquals: { "aws:SourceAccount": Stack.of(this).account },
        ArnLike: {
          "aws:SourceArn": `arn:aws:elasticmapreduce:${Stack.of(this).region}:${Stack.of(this).account}:*`,
        },
      }),
    });

    this.generateServiceRolePolicyStatements().forEach((statement) => {
      serviceRole.addToPolicy(statement);
    });

    return serviceRole;
  }

  private createEngineSecurityGroup(vpc: ec2.IVpc): ec2.SecurityGroup {
    const sg = new ec2.SecurityGroup(this, "EngineSecurityGroup", {
      vpc: vpc,
      description: "EMR Studio Engine",
      allowAllOutbound: true,
    });
    Tags.of(sg).add("for-use-with-amazon-emr-managed-policies", "true");
    return sg;
  }

  private createWorkspaceSecurityGroup(vpc: ec2.IVpc): ec2.SecurityGroup {
    const sg = new ec2.SecurityGroup(this, "WorkspaceSecurityGroup", {
      vpc: vpc,
      description: "EMR Studio Engine",
      allowAllOutbound: false,
    });
    Tags.of(sg).add("for-use-with-amazon-emr-managed-policies", "true");
    sg.addEgressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(443),
      "Required for outbound git access"
    );
    return sg;
  }

  private generateServiceRolePolicyStatements(): iam.PolicyStatement[] {
    const basePolicies = [
      new iam.PolicyStatement({
        sid: "AllowEMRReadOnlyActions",
        effect: iam.Effect.ALLOW,
        actions: [
          "elasticmapreduce:ListInstances",
          "elasticmapreduce:DescribeCluster",
          "elasticmapreduce:ListSteps",
        ],
        resources: ["*"],
      }),
      new iam.PolicyStatement({
        sid: "AllowEC2ENIActionsWithEMRTags",
        effect: iam.Effect.ALLOW,
        actions: [
          "ec2:CreateNetworkInterfacePermission",
          "ec2:DeleteNetworkInterface",
        ],
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
        resources: [
          "arn:aws:ec2:*:*:subnet/*",
          "arn:aws:ec2:*:*:security-group/*",
        ],
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
        sid: "AllowS3GetEncryption",
        effect: iam.Effect.ALLOW,
        actions: ["s3:GetEncryptionConfiguration"],
        resources: [this.bucket.bucketArn],
      }),
    ];

    if (this.bucket.encryptionKey) {
      // Add KSM policies
    }

    // Is ListRoles needed for the service role for runtime roles?
    // Don't think so
    // basePolicies.push(
    //   new iam.PolicyStatement({
    //     sid: "AllowRuntimeRoles",
    //     effect: iam.Effect.ALLOW,
    //     actions: [
    //       "iam:ListRoles",
    //       "iam:GetUser",
    //       "iam:GetRole",
    //       "iam:ListUsers",
    //       "iam:ListRoles",
    //       "sso:GetManagedApplicationInstance",
    //       "sso-directory:SearchUsers",
    //     ],
    //     resources: ["*"],
    //   })
    // );

    if (this.workspaceCollaborationEnabled) {
      basePolicies.push(
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
        })
      );
    }

    return basePolicies;
  }

  /**
   * Managed policy for "basic" user functionality inside studio
   *
   * @returns IAM Managed Policy
   */
  private generateUserPolicy(): iam.ManagedPolicy {
    return new iam.ManagedPolicy(this, "StudioUserPolicy", {
      document: new iam.PolicyDocument({
        statements: this._commonUserPolicies(),
      }),
    });
  }

  private _commonUserPolicies(): iam.PolicyStatement[] {
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
        actions: [
          "ec2:DescribeVpcs",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
        ],
        resources: ["*"],
      }),
      new iam.PolicyStatement({
        sid: "ListIAMRoles",
        actions: ["iam:ListRoles"],
        resources: ["*"],
      }),
    ];
  }
}
