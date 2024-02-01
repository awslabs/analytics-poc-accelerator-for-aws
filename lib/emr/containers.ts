import { KubectlV28Layer } from "@aws-cdk/lambda-layer-kubectl-v28";
import { CfnOutput, IResource, Names, RemovalPolicy, Resource } from "aws-cdk-lib";
import { aws_ec2 as ec2 } from "aws-cdk-lib";
import { aws_emrserverless as emrs } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import {
  Connections,
  ISecurityGroup,
  SecurityGroup,
  SubnetSelection,
} from "aws-cdk-lib/aws-ec2";
import { CfnVirtualCluster } from "aws-cdk-lib/aws-emrcontainers";
import { ManagedPolicy, PolicyDocument, PolicyStatement } from "aws-cdk-lib/aws-iam";
import { processing } from "aws-dsf";
import { Construct } from "constructs";

export interface IVirtualCluster extends IResource, iam.IGrantable {
  /**
   * The application's ID
   *
   * @attribute
   */
  readonly virtualClusterId: string;

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

export interface VirtualClusterProps {

  /**
   * The EKS cluster administrator, useful when a user need to debug through the logs in pod
   */
  readonly adminRole: iam.IRole;

  /**
   * VPC to launch the application in. If no VPC is provided, the cdk application creates
   * a vpc with a 10.0.0.0/16 CIDR and with three public and private subnets
   * The EMR Serverless Application is provided the private subnets.
   *
   * @default - The application is created with a default vpc
   */
  readonly vpc?: ec2.IVpc;

  /**
     * The CIDR blocks that are allowed access to your cluster’s public Kubernetes API server endpoint.
     */
  readonly publicAccessCIDRs: string[];

  readonly createEmrOnEksServiceLinkedRole?: boolean;

}

export class VirtualCluster extends Resource implements IVirtualCluster {


  public readonly virtualCluster: CfnVirtualCluster;
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
  public readonly virtualClusterId: string;

  /**
   * @attribute
   */
  public readonly virtualClusterArn: string;

  constructor(scope: Construct, id: string, props: VirtualClusterProps) {
    super(scope, id);

    //Layer must be changed according to the Kubernetes version used
    const kubectlLayer = new KubectlV28Layer(this, 'kubectlLayer');

    // creation of the construct(s) under test
    const emrEksCluster = processing.SparkEmrContainersRuntime.getOrCreate(this, {
        eksAdminRole: props.adminRole,
        eksClusterName: 'oda-poc',
        publicAccessCIDRs: props.publicAccessCIDRs,
        createEmrOnEksServiceLinkedRole: props.createEmrOnEksServiceLinkedRole,
        kubectlLambdaLayer: kubectlLayer,
        eksVpc: props.vpc,
        removalPolicy: RemovalPolicy.DESTROY,
    });

    let eksNamespace: string = "emrpoc";

    const virtualCluster = emrEksCluster.addEmrVirtualCluster(this, {
        name: 'emrpoc',
        createNamespace: true,
        eksNamespace: eksNamespace,
    });

    const s3Read = new PolicyDocument({
        statements: [new PolicyStatement({
            actions: [
            's3:GetObject',
            ],
            resources: ['arn:aws:s3:::aws-data-analytics-workshops', 'arn:aws:s3:::aws-data-analytics-workshops/*'],
        })],
        });
  
    const s3ReadPolicy = new ManagedPolicy(this, 's3ReadPolicy', {
            document: s3Read,
        });
    
    this.jobRole = emrEksCluster.createExecutionRole(this, 'ExecRoleEmrContainers', s3ReadPolicy, eksNamespace, 's3ReadExecRole');
    
    this.grantPrincipal = this.jobRole;

    new CfnOutput(this, "podTemplateS3LocationDriverShared", {
        value: emrEksCluster.podTemplateS3LocationDriverShared!,
        description: "S3 location for driver pod template",
      });

    new CfnOutput(this, "podTemplateS3LocationExecutorShared", {
        value: emrEksCluster.podTemplateS3LocationExecutorShared!,
        description: "S3 location for executor pod template",
      });
        
    this.virtualClusterId = virtualCluster.attrId;
    this.virtualClusterArn = virtualCluster.attrArn;
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
        "emr-containers:ListJobRuns",
        "iam:ListRoles",
      ],
      resourceArns: ["*"],
    });

    iam.Grant.addToPrincipal({
      grantee: identity,
      actions: [
        "emr-containers:CancelJobRun",
        "emr-containers:ListJobRuns",
        "emr-containers:GetJobRun"
      ],
      resourceArns: [this.virtualClusterArn, `${this.virtualClusterArn}/jobruns/*`],
    });

    return iam.Grant.addToPrincipal({
      grantee: identity,
      actions: ["emr-containers:StartJobRun"],
      resourceArns: [this.virtualClusterArn],
      conditions: {
        ArnEquals: { "emr-containers:ExecutionRoleArn": [this.jobRole.roleArn] },
      },
    });
  }

}
