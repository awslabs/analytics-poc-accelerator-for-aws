import { IResolvable, IResource, Names, Resource } from "aws-cdk-lib";
import { aws_ec2 as ec2 } from "aws-cdk-lib";
import { aws_emrserverless as emrs } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import {
  Connections,
  ISecurityGroup,
  SecurityGroup,
  SubnetSelection,
} from "aws-cdk-lib/aws-ec2";
import { CfnApplication } from "aws-cdk-lib/aws-emrserverless";
import { processing } from "aws-dsf";
import { Construct } from "constructs";
import * as semver from "semver";

interface MySparkEmrServerlessRuntimeProps
  extends processing.SparkEmrServerlessRuntimeProps {
  readonly runtimeConfiguration?:
    | IResolvable
    | emrs.CfnApplication.ConfigurationObjectProperty[];
}
class MySparkEmrServerlessRuntime extends processing.SparkEmrServerlessRuntime {
  constructor(
    scope: Construct,
    id: string,
    props: MySparkEmrServerlessRuntimeProps
  ) {
    super(scope, id, props);
  }
}

export interface IApplication extends IResource, iam.IGrantable {
  /**
   * The application's ID
   *
   * @attribute
   */
  readonly applicationId: string;

  /**
   * Manage the allowed network connections for the application with Security Groups.
   */
  readonly connections: ec2.Connections;

  /**
   * Grant management permissions for this application to an IAM principal (Role/Group/User).
   *
   * Includes the ability to start/stop the application, create a Serverless console,
   * and run jobs with the job role.
   *
   * @param identity The principal
   * @param objectsKeyPattern Restrict the permission to a certain key pattern (default '*')
   */
  grantManage(identity: iam.IGrantable): iam.Grant;
}

export interface ApplicationProps {
  /**
   * VPC to launch the application in. If no VPC is provided, the cdk application creates
   * a vpc with a 10.0.0.0/16 CIDR and with three public and private subnets
   * The EMR Serverless Application is provided the private subnets.
   *
   * @default - The application is created with a default vpc
   */
  readonly vpc?: ec2.IVpc;

  /**
   * Where to place the application within the VPC, if no subnet is provided
   * the cdk application creates a vpc with a 10.0.0.0/16 CIDR and with three public and private subnets
   * The EMR Serverless Application is provided the private subnets.
   *
   * @default - Private subnets
   */
  readonly vpcSubnets?: SubnetSelection;

  /**
   * Security Group to assign to this application
   *
   * @default - create new security group, only if `vpc` is provided.
   */
  readonly securityGroup?: ISecurityGroup;

  /**
   * An IAM role to use when running jobs on the application.
   *
   * @default - A role will automatically be created, it can be accessed via the `jobRole` property
   */
  readonly jobRole?: iam.IRole;

  /**
   * The EMR release label, such as emr-6.12.0.
   *
   * @default - The latest supported EMR version
   */
  readonly releaseLabel?: processing.EmrRuntimeVersion;
}

export class Application extends Resource implements IApplication {
  /**
   * The underlying EMR Serverless application
   */
  public readonly application: emrs.CfnApplication;

  public readonly connections: ec2.Connections;

  /**
   * The IAM role that can be used by jobs.
   */
  public readonly jobRole: iam.IRole;

  /**
   * The principal to grant permissions to.
   */
  public readonly grantPrincipal: iam.IPrincipal;

  /**
   * @attribute
   */
  public readonly applicationId: string;

  /**
   * @attribute
   */
  public readonly applicationArn: string;

  /**
   * The managed policy that allows EMR Studio users to connect to
   * this application.
   *
   * https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/interactive-workloads.html
   */
  private studioAccessPolicy: iam.IManagedPolicy;

  constructor(scope: Construct, id: string, props: ApplicationProps) {
    super(scope, id);

    const releaseLabel: processing.EmrRuntimeVersion =
      props.releaseLabel ?? processing.EMR_DEFAULT_VERSION;

    const releaseLabelSemver: string = releaseLabel.split("-")[1];

    if (semver.lt(releaseLabelSemver, "6.9.0")) {
      throw new Error(
        `EMR Serverless supports release EMR 6.9 and above, provided release is ${releaseLabel.toString()}`
      );
    }

    let networkConfiguration:
      | CfnApplication.NetworkConfigurationProperty
      | undefined;
    let securityGroup: ISecurityGroup | undefined = undefined;

    if (props.vpc) {
      securityGroup =
        props.securityGroup ??
        new ec2.SecurityGroup(this, "SecurityGroup", {
          vpc: props.vpc,
          allowAllOutbound: true,
        });
      networkConfiguration = {
        subnetIds: props.vpc.selectSubnets(props.vpcSubnets).subnetIds,
        securityGroupIds: [securityGroup.securityGroupId],
      };
    }

    let emrServerlessApplication = new MySparkEmrServerlessRuntime(
      this,
      Names.uniqueId(this),
      {
        name: "emr-poc",
        releaseLabel: releaseLabel,
        networkConfiguration: networkConfiguration,
        runtimeConfiguration: [
          {
            classification: "spark-defaults",
            properties: {
              "spark.hadoop.hive.metastore.client.factory.class":
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
              "spark.sql.catalogImplementation": "hive",
            },
          },
        ],
      }
    );

    this.jobRole =
      props.jobRole ??
      processing.SparkEmrServerlessRuntime.createExecutionRole(this, "ExecutionRole");

    this.grantPrincipal = this.jobRole;

    this.applicationId = emrServerlessApplication.application.attrApplicationId;
    this.applicationArn = emrServerlessApplication.application.attrArn;

    this.connections = new Connections({
      securityGroups: [
        securityGroup! ?? emrServerlessApplication.emrApplicationSecurityGroup,
      ],
    });
  }

  /**
   * Grant management permissions for this application to an IAM principal (Role/Group/User).
   *
   * Includes the ability to start/stop the application, create a Serverless console,
   * and run jobs with the job role.
   *
   * @param identity The principal
   * @param objectsKeyPattern Restrict the permission to a certain key pattern (default '*')
   */
  public grantManage(identity: iam.IGrantable): iam.Grant {
    iam.Grant.addToPrincipal({
      grantee: identity,
      actions: [
        "elasticmapreduce:CreateStudioPresignedUrl",
        "elasticmapreduce:DescribeStudio",
        "elasticmapreduce:CreateStudio",
        "elasticmapreduce:ListStudios",
        "emr-serverless:ListApplications",
        "iam:ListRoles",
      ],
      resourceArns: ["*"],
    });

    iam.Grant.addToPrincipal({
      grantee: identity,
      actions: [
        "emr-serverless:GetApplication",
        "emr-serverless:StartApplication",
        "emr-serverless:StopApplication",
        "emr-serverless:StartJobRun",
        "emr-serverless:CancelJobRun",
        "emr-serverless:ListJobRuns",
        "emr-serverless:GetJobRun",
        "emr-serverless:GetDashboardForJobRun",
      ],
      resourceArns: [this.applicationArn, `${this.applicationArn}/jobruns/*`],
    });

    iam.Grant.addToPrincipal({
      grantee: identity,
      actions: ["ec2:CreateNetworkInterface"],
      resourceArns: ["arn:aws:ec2:*:*:network-interface/*"],
      conditions: {
        StringEquals: {
          "aws:CalledViaLast": "ops.emr-serverless.amazonaws.com",
        },
      },
    });

    return iam.Grant.addToPrincipal({
      grantee: identity,
      actions: ["iam:PassRole"],
      resourceArns: [this.jobRole.roleArn],
      conditions: {
        StringLike: { "iam:PassedToService": "emr-serverless.amazonaws.com" },
      },
    });
  }

  public getStudioAccessPolicy(): iam.IManagedPolicy {
    // TODO: Throw a warning here if interactive access isn't enabled
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
              sid: "EMRServerlessInteractiveAccess",
              actions: ["emr-serverless:AccessInteractiveEndpoints"],
              resources: [this.applicationArn],
            }),
            new iam.PolicyStatement({
              sid: "EMRServerlessRuntimeRoleAccess",
              actions: ["iam:PassRole"],
              resources: [this.jobRole.roleArn],
              conditions: {
                StringLike: {
                  "iam:PassedToService": "emr-serverless.amazonaws.com",
                },
              },
            }),
          ],
        }),
      }
    );

    return this.studioAccessPolicy;
  }
}
