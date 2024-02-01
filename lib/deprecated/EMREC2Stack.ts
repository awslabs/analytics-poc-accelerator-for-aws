import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { aws_iam as iam } from "aws-cdk-lib";
import { aws_ec2 as ec2 } from "aws-cdk-lib";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { aws_s3_deployment as s3deploy } from "aws-cdk-lib";
import { aws_emr as emr } from "aws-cdk-lib";
import { PermissionsStack } from "../PermissionsStack";
import path = require("path");

export interface EMREC2StackProps extends cdk.StackProps {
  readonly vpc: ec2.IVpc;
  readonly bucket: s3.IBucket;
  readonly permissions: PermissionsStack;
}

export class EMREC2Stack extends cdk.Stack {
  // EMR Studio can use this role to connect with a runtime role
  public readonly jobRole: iam.Role;

  constructor(scope: Construct, id: string, props: EMREC2StackProps) {
    super(scope, id, props);

    // This cluster is created with support for [EMR Runtime Roles](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-steps-runtime-roles.html)
    // Access to use the runtime role for steps is granted to the Data Engineer user
    // https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-steps-runtime-roles.html
    // We create 3 roles below:
    // 1. An EMR EC2 service role for cluster operations
    // 2. An EMR EC2 instance profile (and role) that's used to kick off the job
    // 3. An EMR EC2 job role that has only the data access necessary to run a job
    // In theory the job role has the same permissions as EMR Serverless, but due to the extra permissions necessary
    // to allow for Step execution and Studio access, 2 different roles are created.

    // 1. Service role: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role.html
    const EMREC2ServiceRole = new iam.Role(this, "emr-ec2-service-role", {
      assumedBy: new iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonEMRServicePolicy_v2")],
    });

    // 2. Instance role and profile: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role-for-ec2.html
    const emrEC2InstanceRole = new iam.Role(this, "emr-ec2-instance-role", {
      assumedBy: new iam.ServicePrincipal("ec2.amazonaws.com"),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonSSMManagedInstanceCore")],
    });
    const emrEC2InstanceProfile = new iam.CfnInstanceProfile(this, "emr-ec2-instance-profile", {
      instanceProfileName: emrEC2InstanceRole.roleName,
      roles: [emrEC2InstanceRole.roleName],
    });
    props.bucket.grantWrite(emrEC2InstanceRole, "logs/emr/*");
    props.bucket.grantRead(emrEC2InstanceRole, "emr/*");

    // 3. Job role: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-steps-runtime-roles.html
    this.jobRole = new iam.Role(this, "oda-poc-emr-ec2-job-role", {
      assumedBy: new iam.ArnPrincipal(emrEC2InstanceRole.roleArn),
      managedPolicies: [props.permissions.defaultDataAccessPolicy],
    });
    new cdk.CfnOutput(this, "emr-ec2-job-role-arn", { value: this.jobRole.roleArn });

    emrEC2InstanceRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["sts:AssumeRole", "sts:TagSession"],
        resources: [this.jobRole.roleArn],
      })
    );

    // FIXME: ONLY FOR DEMO PURPOSES FOR EMR STUDIO USER TO USE `emr run` COMMAND
    // this.jobRole.addToPolicy(
    //   new iam.PolicyStatement({
    //     actions: ["emr-serverless:*", "glue:*"],
    //     resources: ["*"]
    //   })
    // );
    // this.jobRole.addToPolicy(
    //   new iam.PolicyStatement({
    //     sid: "EMRServerlessPassRole",
    //     effect: iam.Effect.ALLOW,
    //     actions: ["iam:PassRole"],
    //     resources: ["arn:aws:iam::568026268536:role/EMRServerlessStack-EMRServerlessJobRole799E15F3-Z5J6M2T7ZE0B"],
    //     conditions: { StringLike: { "iam:PassedToService": "emr-serverless.amazonaws.com" } },
    //   }),
    // )

    // EMR needs to be able to PassRole to the instance profile role
    // https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role-for-ec2.html#emr-ec2-role-least-privilege
    // https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role.html
    EMREC2ServiceRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["iam:PassRole"],
        resources: [emrEC2InstanceRole.roleArn],
        conditions: { StringEquals: { "iam:PassedToService": "ec2.amazonaws.com" } },
      })
    );

    // With that all done, allow the data engineer user the ability to call add steps with the runtime role.
    const addStepsPolicy = new iam.ManagedPolicy(this, "emrec2-basic-user-policy", {
      document: new iam.PolicyDocument({
        statements: this._runtimeRolePolicies(this.jobRole),
      }),
    });
    addStepsPolicy.attachToUser(props.permissions.engineerUser);

    const certFile = new s3deploy.BucketDeployment(this, "EMREC2Deployment", {
      sources: [s3deploy.Source.asset("./assets/certs/my-certs.zip")],
      destinationBucket: props.bucket,
      extract: false,
      destinationKeyPrefix: "emr/certs/",
    });
    const certLocation = props.bucket.s3UrlForObject(path.join("emr/certs", cdk.Fn.select(0, certFile.objectKeys)));

    // Now create our EMR cluster
    // We need a security configuration to enable runtime roles
    // In-Transit encryption is required for connections from EMR Studio
    const emrClusterSecConfig = new emr.CfnSecurityConfiguration(this, "EMREC2RuntimeConfig", {
      securityConfiguration: {
        AuthorizationConfiguration: {
          IAMConfiguration: {
            EnableApplicationScopedIAMRole: true,
          },
        },
        EncryptionConfiguration: {
          EnableInTransitEncryption: true,
          EnableAtRestEncryption: false,
          InTransitEncryptionConfiguration: {
            TLSCertificateConfiguration: {
              CertificateProviderType: "PEM",
              S3Object: certLocation,
            },
          },
        },
      },
    });

    const cluster = new emr.CfnCluster(this, "EMREC2Cluster", {
      name: "emr-poc",
      releaseLabel: "emr-6.12.0",
      applications: [{ name: "Spark" }, { name: "Livy" }, { name: "JupyterEnterpriseGateway" }],
      logUri: `s3://${props.bucket.bucketName}/logs/emr/`,
      securityConfiguration: emrClusterSecConfig.ref,
      serviceRole: EMREC2ServiceRole.roleArn,
      jobFlowRole: emrEC2InstanceRole.roleName,
      instances: {
        keepJobFlowAliveWhenNoSteps: true,
        ec2SubnetIds: props.vpc.selectSubnets({ subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS }).subnetIds,
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
      autoTerminationPolicy: { idleTimeout: 14400 },
      tags: [
        {
          key: "for-use-with-amazon-emr-managed-policies",
          value: "true",
        },
      ],
    });
    cluster.addDependency(emrEC2InstanceProfile);

    new cdk.CfnOutput(this, "emrEC2Cluster", {
      value: cluster.ref,
    });
  }

  /**
   * Policy statements to allow a user to use runtime roles with Steps
   *
   * @param role The role to grant access to
   */
  _runtimeRolePolicies(role: iam.Role): iam.PolicyStatement[] {
    return [
      new iam.PolicyStatement({
        sid: "AddStepsWithSpecificExecRoleArn",
        actions: ["elasticmapreduce:AddJobFlowSteps"],
        resources: ["*"],
        conditions: { StringEquals: { "elasticmapreduce:ExecutionRoleArn": [role.roleArn] } },
      }),
    ];
  }
}
