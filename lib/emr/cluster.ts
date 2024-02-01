import { IResource, RemovalPolicy, Resource } from "aws-cdk-lib";
import { aws_ec2 as ec2 } from "aws-cdk-lib";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { aws_emr as emr } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { CfnCluster } from "aws-cdk-lib/aws-emr";
import { Construct } from "constructs";
import { EMR } from "@aws-sdk/client-emr";
import { SelfSignedCertificateResource } from "./custom-certgen";

const emrClient = new EMR({});

export interface ICluster extends IResource, iam.IGrantable {
  /**
   * The cluster's ID
   *
   * @attribute
   */
  readonly clusterId: string;

  /**
   * Manage the allowed network connections for the cluster with Security Groups.
   */
  readonly connections: ec2.Connections;
}

/**
 * A set of options for configuring runtime roles on EMR.
 *
 * An existing role can be provided or the role can be created at deploy time.
 * Additionally, if SageMaker Studio or EMR Studio access is required, use
 * `withStudioAccess` to enable in-transit encryption on EMR with self-signed certs.
 *
 * If an existing `securityConfiguration` is provided when creating the cluster, that
 * configuration will be used instead.
 */
export interface RuntimeRoleOptions {
  /**
   * An IAM role to use when running jobs on the application.
   * https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-steps-runtime-roles.html
   *
   * * The role must be assumable by the `instanceRole` used with the cluster.
   *
   * @default - A role will automatically be created if `createJobRole` is passed.
   *            It can be accessed via the `jobRole` property
   */
  readonly jobRole?: iam.IRole;

  /**
   * Enable the creasion of an [EMR runtime role](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-steps-runtime-roles.html).
   * This allows for each job to have a different set of access control.
   *
   * When enabled along with SageMaker Studio or EMR Studio, the EMR cluster *must* have in-transit encryption enabled as well.
   */
  readonly createRuntimeRole?: boolean;

  /**
   * When a `securityConfiguration` is not provided, create a new security configuration with in-transit encryption enabled.
   * Additionally, create a set of self-signed certificates (NOT TO BE USED IN PRODUCTION) to use by default.
   */
  readonly withStudioAccess?: boolean;
}

export interface ClusterProps {
  /**
   * VPC to launch the cluster in.
   *
   * The VPC must be tagged with for-use-with-amazon-emr-managed-policies=true when using EMR default managed policies.
   */
  readonly vpc: ec2.IVpc;

  /**
   * An IAM role used by Amazon EMR when provisioning resources and performing service-level tasks.
   * https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role.html
   *
   * The role must be assumable by the service principal `elasticmapreduce.amazonaws.com`:
   *
   * @example
   * const role = new iam.Role(this, 'MyRole', {
   *   assumedBy: new iam.ServicePrincipal('elasticmapreduce.amazonaws.com'),
   *   managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonEMRServicePolicy_v2")],
   * });
   *
   * @default - A role will automatically be created, it can be accessed via the `serviceRole` property
   */
  readonly serviceRole?: iam.IRole;

  /**
   * An IAM role to associate with the instance profile assigned to cluster instances.
   * https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role-for-ec2.html
   *
   * * The role must be assumable by the service principal `ec2.amazonaws.com`:
   *
   * @example
   * const role = new iam.Role(this, 'MyRole', {
   *   assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com')
   * });
   *
   * @default - A role will automatically be created, it can be accessed via the `instanceRole` property
   */
  readonly instanceRole?: iam.IRole;

  /**
   * A set of options for enabling and using EMR runtime roles.
   *
   * If a role is provided or created and no `securityConfigurationName` is provided,
   * a security configuration will be created for this deployment.
   *
   * If Studio access is requested, the security configuration will be configured to use
   * in-transit encryption and a set of self-signed certificates will be generated and
   * uploaded to S3.
   */
  readonly runtimeRoleOptions?: RuntimeRoleOptions;

  /**
   * Add SSM session permissions to the instance role
   *
   * Setting this to `true` adds the necessary permissions to connect
   * to the instance using SSM Session Manager. You can do this
   * from the AWS Console.
   *
   * NOTE: Setting this flag to `true` may not be enough by itself.
   * You must also use an AMI that comes with the SSM Agent, or install
   * the SSM Agent yourself. See
   * [Working with SSM Agent](https://docs.aws.amazon.com/systems-manager/latest/userguide/ssm-agent.html)
   * in the SSM Developer Guide.
   *
   * @default false
   */
  readonly ssmSessionPermissions?: boolean;

  /**
   * The security configuration name to apply to the cluster.
   *
   * @default - A security configuration with in-transit encryption enabled with a self-signed certificate.
   */
  readonly securityConfigurationName?: string;

  /**
   * The amount of idle time in seconds after which a cluster automatically terminates.
   *
   * @default - No idle timeout.
   */
  readonly idleTimeout?: number;

  /**
   * The name of the S3 bucket to use for EMR and application logs.
   *
   * @default - Logging is not enabled.
   */
  readonly loggingBucket?: s3.IBucket;

  /**
   * The prefix to use for EMR service logs.
   *
   * @default - logs/
   */
  readonly loggingBucketPrefix?: string;

  /**
   * The name of the S3 bucket to use for EMR artifacts
   * like bootstrap actions and certificate files.
   *
   * @default - Logging is not enabled.
   */
  readonly artifactsBucket?: s3.IBucket;

  /**
   * The prefix to use for EMR artifacts.
   *
   * @default - code/
   */
  readonly artifactsBucketPrefix?: string;

  //   /**
  //    * The EC2 Instance Type to use for nodes.
  //    *
  //    * @default - m5.xlarge
  //    */
  //   readonly instanceType?: string;

  //   /**
  //    * The EC2 KeyPair to use for instances.
  //    *
  //    * @default - No EC2 KeyPair
  //    */
  //   readonly keyPair?: ec2.IKeyPair;

  //   /**
  //    * The number of nodes to create in the cluster.
  //    *
  //    * @default - 3
  //    */
  //   readonly numNodes?: number;

  //   /**
  //    * The autoscaling policy for the node groups in the cluster.
  //    *
  //    * @default - Default autoscaling policy
  //    */
  //   readonly autoscaling?: emr.AutoScalingPolicy;

  /**
   * The EMR release label, such as emr-6.12.0.
   *
   * @default - The latest supported EMR version
   */
  readonly releaseLabel?: string;

  //   /**
  //    * The autoscaling role, which is used by automatic scaling.
  //    */

  /**
   * Determine what happens to the cluster when the resource/stack is deleted.
   *
   * @default RemovalPolicy.DESTROY
   */
  readonly removalPolicy?: RemovalPolicy;
}

export class Cluster extends Resource implements ICluster {
  public readonly connections: ec2.Connections;

  /**
   * The underlying EMR cluster
   */
  public readonly cluster: CfnCluster;

  /**
   * @attribute
   */
  public readonly clusterId: string;

  /**
   * The IAM role used by EMR for service-level tasks.
   */
  public readonly serviceRole: iam.IRole;

  /**
   * The IAM role assumed by underlying cluster instances.
   */
  public readonly instanceRole: iam.IRole;

  /**
   * The IAM role used to run jobs.
   */
  public readonly jobRole?: iam.IRole;

  /**
   * The principal to grant permissions to. Runtime role, if enabled, or
   * instance role.
   */
  public readonly grantPrincipal: iam.IPrincipal;

  /**
   * Prefix where logs will be written in S3 bucket, if provided
   *
   */
  private readonly loggingBucketPrefix: string;

  /**
   * The managed policy that allows EMR Studio users to use runtime roles
   * with this cluster.
   *
   * https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-studio-runtime.html
   */
  private studioAccessPolicy: iam.IManagedPolicy;

  constructor(scope: Construct, id: string, props: ClusterProps) {
    super(scope, id);

    // this.connections = vpc.selectSubnets({
    //   subnetType: ec2.SubnetType.PRIVATE_WITH_NAT,
    // }).connections;

    this.loggingBucketPrefix = props.loggingBucketPrefix ?? "logs/emr/";

    this.serviceRole =
      props.serviceRole ??
      new iam.Role(this, "ServiceRole", {
        assumedBy: new iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName(
            "service-role/AmazonEMRServicePolicy_v2"
          ),
        ],
      });

    this.instanceRole =
      props.instanceRole ??
      new iam.Role(this, "InstanceRole", {
        assumedBy: new iam.ServicePrincipal("ec2.amazonaws.com"),
      });

    // The instance role needs access to write to EMR logs
    if (props.instanceRole === undefined && props.loggingBucket) {
      props.loggingBucket.grantWrite(
        this.instanceRole,
        `${this.loggingBucketPrefix}*`
      );
    }

    if (props.ssmSessionPermissions) {
      this.instanceRole.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "AmazonSSMManagedInstanceCore"
        )
      );
    }

    let securityConfig: emr.CfnSecurityConfiguration | undefined = undefined;

    // If a jobRole is passed in *or* createJobRole is passed in, create or use the role.
    // TODO: Set default for createRuntimeRole to be true
    // TODO: Revist the logic here - we may just want to _always_ connect the runtime role to the instance role.
    if (
      props.runtimeRoleOptions?.jobRole ||
      props.runtimeRoleOptions?.createRuntimeRole
    ) {
      this.jobRole =
        props.runtimeRoleOptions?.jobRole ??
        new iam.Role(this, "JobRole", {
          assumedBy: new iam.ArnPrincipal(this.instanceRole.roleArn),
        });

      // The instance role, if created, also needs access to assume the runtime role
      if (
        props.instanceRole === undefined &&
        props.runtimeRoleOptions?.createRuntimeRole == true
      ) {
        this.instanceRole.addToPrincipalPolicy(
          new iam.PolicyStatement({
            actions: ["sts:AssumeRole", "sts:TagSession"],
            resources: [this.jobRole.roleArn],
          })
        );
      }

      // If no security config is provided, create the base options here
      const securityConfiguration = {
        AuthorizationConfiguration: {},
        EncryptionConfiguration: {
          EnableInTransitEncryption: false,
          EnableAtRestEncryption: false,
          InTransitEncryptionConfiguration: {},
        },
      };
      if (props.securityConfigurationName === undefined) {
        securityConfiguration.AuthorizationConfiguration = {
          IAMConfiguration: {
            EnableApplicationScopedIAMRole: true,
          },
        };

        if (props.runtimeRoleOptions.withStudioAccess) {
          if (props.artifactsBucket === undefined) {
            throw new Error(
              "artifactsBucket property must be provide to enable runtime roles with Studio access"
            );
          }
          const cert = new SelfSignedCertificateResource(
            this,
            "SelfSignedCert",
            {
              region: this.env.region,
              bucket: props.artifactsBucket!,
              prefix: "emr/certs/",
            }
          );
          props.artifactsBucket.grantRead(this.instanceRole, "emr/certs/*");
          securityConfiguration.EncryptionConfiguration = {
            EnableInTransitEncryption: true,
            EnableAtRestEncryption: false,
            InTransitEncryptionConfiguration: {
              TLSCertificateConfiguration: {
                CertificateProviderType: "PEM",
                S3Object: cert.s3Url,
              },
            },
          };
        }

        securityConfig = new emr.CfnSecurityConfiguration(
          this,
          "EMREC2RuntimeConfig",
          { securityConfiguration }
        );
      }
    }

    // Set the grant principal to the job role associated with the cluster
    // or the instance role
    this.grantPrincipal = this.jobRole ?? this.instanceRole;

    // TODO: FIXME Temporarily add Glue access to the instance profile
    this.instanceRole.addManagedPolicy(
      new iam.ManagedPolicy(this, "StudioUserPolicy", {
        document: new iam.PolicyDocument({
          statements: [this._glueReadOnlyPolicy(), this._glueWritePolicy()],
        }),
      })
    );

    // Explicitly specify the instanceProfileName because when used with runtime roles,
    // the role name has to match the instance profile name as the trust policies
    // can only be applied to roles and not instance profiles.
    const iamProfile = new iam.CfnInstanceProfile(this, "InstanceProfile", {
      instanceProfileName: this.instanceRole.roleName,
      roles: [this.instanceRole.roleName],
    });

    // If a new service role or instance role was created,
    // ensure the service role passes role to the instance role.
    if (!props.serviceRole || !props.instanceRole) {
      this.serviceRole.addToPrincipalPolicy(
        new iam.PolicyStatement({
          actions: ["iam:PassRole"],
          resources: [this.instanceRole.roleArn],
          conditions: {
            StringEquals: { "iam:PassedToService": "ec2.amazonaws.com" },
          },
        })
      );
    }

    // The basic required cluster settings - in the future, allow more flexibility
    const clusterSettings = {
      name: "emr-poc",
      releaseLabel: props.releaseLabel ?? "emr-6.15.0",
      applications: [
        { name: "Spark" },
        { name: "Livy" },
        { name: "JupyterEnterpriseGateway" },
        { name: "Hadoop" },
        { name: "Hive" },
      ],
      serviceRole: this.serviceRole.roleArn,
      jobFlowRole: this.instanceRole.roleName,

      instances: {
        keepJobFlowAliveWhenNoSteps: true,
        ec2SubnetIds: props.vpc.selectSubnets({
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        }).subnetIds,
        masterInstanceFleet: {
          name: "Primary",
          targetOnDemandCapacity: 1,
          targetSpotCapacity: 0,
          instanceTypeConfigs: [
            { instanceType: "r5.2xlarge" },
            { instanceType: "r5b.2xlarge" },
            { instanceType: "r5d.2xlarge" },
            { instanceType: "r5a.2xlarge" },
          ],
        },
        coreInstanceFleet: {
          name: "Core",
          targetOnDemandCapacity: 0,
          targetSpotCapacity: 1,
          instanceTypeConfigs: [
            { instanceType: "c5a.2xlarge" },
            { instanceType: "m5a.2xlarge" },
            { instanceType: "r5a.2xlarge" },
          ],
          launchSpecifications: {
            onDemandSpecification: {
              allocationStrategy: "lowest-price",
            },
            spotSpecification: {
              timeoutDurationMinutes: 10,
              timeoutAction: "SWITCH_TO_ON_DEMAND",
              allocationStrategy: "capacity-optimized",
            },
          },
        },
      },

      autoTerminationPolicy: props.idleTimeout
        ? { idleTimeout: props.idleTimeout }
        : undefined,
      securityConfiguration:
        props.securityConfigurationName ?? securityConfig?.ref,
      logUri: props.loggingBucket
        ? props.loggingBucket.s3UrlForObject(this.loggingBucketPrefix)
        : undefined,

      configurations: [
        {
          classification: "spark-hive-site",
          properties: {
            "hive.metastore.client.factory.class":
              "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
          },
        },
      ],

      tags: [
        {
          key: "for-use-with-amazon-emr-managed-policies",
          value: "true",
        },
      ],
    };

    this.cluster = new emr.CfnCluster(this, "EMREC2Cluster", clusterSettings);
    this.clusterId = this.cluster.ref;
    this.cluster.addDependency(iamProfile);
    securityConfig && this.cluster.addDependency(securityConfig);

    this.cluster.applyRemovalPolicy(
      props.removalPolicy ?? RemovalPolicy.DESTROY
    );
  }

  /**
   * Create a new runtime role intended to be used with this cluster.
   * The instance role associated with this cluster is allowed to assume the role.
   *
   * @returns iam.IRole - The newly created runtime role
   */
  public createRuntimeRole(): iam.IRole {
    const role = new iam.Role(this, "JobRole", {
      assumedBy: new iam.ArnPrincipal(this.instanceRole.roleArn),
    });

    this.instanceRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: ["sts:AssumeRole", "sts:TagSession"],
        resources: [role.roleArn],
      })
    );

    return role;
  }

  public getStudioAccessPolicy(): iam.IManagedPolicy {
    // TODO: Throw a warning here if runtime roles aren't enabled
    if (this.studioAccessPolicy) {
      return this.studioAccessPolicy;
    }
    this.studioAccessPolicy = new iam.ManagedPolicy(
      this,
      "StudioAccessPolicy",
      {
        document: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              sid: "AllowSpecificExecRoleArn",
              actions: ["elasticmapreduce:GetClusterSessionCredentials"],
              resources: ["*"],
              conditions: {
                StringEquals: {
                  "elasticmapreduce:ExecutionRoleArn": [this.jobRole?.roleArn],
                },
              },
            }),
          ],
        }),
      }
    );

    return this.studioAccessPolicy;
  }

  _glueReadOnlyPolicy() {
    return new iam.PolicyStatement({
      sid: "GlueReadaccess",
      actions: [
        "glue:GetDatabase",
        "glue:GetDataBases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetTableVersion",
        "glue:GetTableVersions",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:BatchGetPartition",
        "glue:GetUserDefinedFunctions",
        "glue:SearchTables",
      ],
      resources: ["*"],
    });
  }

  _glueWritePolicy() {
    return new iam.PolicyStatement({
      sid: "GlueWriteAccess",
      actions: [
        "glue:CreateDatabase",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:CreatePartition",
        "glue:BatchCreatePartition",
      ],
      resources: ["*"],
    });
  }

  /**
   * Grants access to list, describe, and launch application UIs for this cluster in the console.
   * Permissions for this are hard to come by, but some are documented here:
   * https://aws.amazon.com/blogs/big-data/best-practices-for-securing-amazon-emr/
   *
   * @param grantee
   * @returns iam.Grant
   */
  public grantConsoleAccess(grantee: iam.IGrantable): iam.Grant {
    const resourceGrant = iam.Grant.addToPrincipal({
      grantee,
      actions: [
        "elasticmapreduce:ListInstance*",
        "elasticmapreduce:GetAutoTerminationPolicy",
        "elasticmapreduce:ListSteps",
        "elasticmapreduce:ListBootstrapActions",
        "elasticMapreduce:GetManagedScalingPolicy",
        "elasticmapreduce:GetOnClusterAppUIPresignedURL",
        "elasticmapreduce:GetPersistentAppUIPresignedURL",
      ],
      resourceArns: [
        this.stack.formatArn({
          service: "elasticmapreduce",
          resource: "cluster",
          resourceName: this.cluster.ref,
        }),
      ],
    });

    const grant = iam.Grant.addToPrincipal({
      grantee,
      actions: [
        "elasticmapreduce:ListReleaseLabels",
        "elasticmapreduce:ListClusters",
        "elasticmapreduce:ListSupportedInstanceTypes",
        "iam:ListRoles",
        "s3:ListBuckets",
        "ec2:DescribeInstanceType*",
        "ec2:DescribeSpotPriceHistory",
        "logs:DescribeMetricFilters",
        "cloudwatch:GetMetricData",
      ],
      resourceArns: ["*"],
    });

    return resourceGrant.combine(grant);
  }
}

/**
 * Fetch the latest EMR release label from the API.
 *
 * May not be able to do this due to the deterministic nature of CDK.
 * See https://github.com/aws/aws-cdk/issues/8273#issuecomment-891829539 for more info
 *
 */
async function fetchLatestReleaseLabel(): Promise<string> {
  const response = await emrClient.listReleaseLabels({});
  return response.ReleaseLabels ? response.ReleaseLabels[0] : "emr-6.13.0";
}
