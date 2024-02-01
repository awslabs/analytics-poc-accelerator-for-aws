import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { aws_ec2 as ec2 } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { aws_emrserverless as emrserverless } from "aws-cdk-lib";
import { PermissionsStack } from "./PermissionsStack";

export interface EMRServerlessStackProps extends cdk.StackProps {
  readonly vpc: ec2.IVpc;
  readonly bucket: s3.IBucket;
  readonly permissions: PermissionsStack;
  readonly dataEngineerUser: iam.User;
  readonly releaseVersion?: string;
}

export class EMRServerlessStack extends cdk.Stack {
  private _application: emrserverless.CfnApplication;
  private _jobRole: iam.Role;

  constructor(scope: Construct, id: string, props: EMRServerlessStackProps) {
    super(scope, id, props);

    const emrServerlessAppName = "emrs-poc";
    const emrServerlessreleaseVersion = props.releaseVersion ? props.releaseVersion : "emr-6.10.0";

    // EMR Serverless application and resources - we utilize a VPC for outbound access
    this._application = new emrserverless.CfnApplication(this, "EMRServerlessApplication", {
      releaseLabel: emrServerlessreleaseVersion,
      type: "SPARK",
      name: emrServerlessAppName,
      networkConfiguration: {
        subnetIds: props.vpc.selectSubnets({ subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS }).subnetIds,
        securityGroupIds: [new ec2.SecurityGroup(this, "emr-serverless-sg", { vpc: props.vpc }).securityGroupId],
      },
    });

    // Create a job role for EMR Serverless
    // 1. Use the pre-defined managed policy from permissions stack for data access
    this._jobRole = new iam.Role(this, "EMRServerlessJobRole", {
      assumedBy: new iam.ServicePrincipal("emr-serverless.amazonaws.com"),
      managedPolicies: [props.permissions.defaultDataAccessPolicy],
    });
    // Note that if we try to use `attachToRole` on the data access policy, we run into
    // cyclic dependency issues: https://github.com/aws/aws-cdk/issues/11020#issuecomment-1516618969
    new cdk.CfnOutput(this, "emrServerlessJobRoleArn", { value: this._jobRole.roleArn });

    // Then grant the POC Data Engineer user access to
    // 1. Create an EMR Studio to view EMR Serverless applications and job UI
    // 2. Manage the created EMR Serverless Application and start/run jobs
    // 3. Use pass role privileges to submit jobs with the above role
    const manageEMRServerless = this._manageEMRServerlessPolicy(this._application, this._jobRole);
    manageEMRServerless.attachToUser(props.permissions.engineerUser);
  }

  _manageEMRServerlessPolicy(emrs: emrserverless.CfnApplication, jobRole: iam.Role): iam.ManagedPolicy {
    return new iam.ManagedPolicy(this, "oda-poc-emrs-dataeng-policy", {
      document: new iam.PolicyDocument({
        statements: [
          new iam.PolicyStatement({
            sid: "EMRServerlessStudioAccess",
            actions: [
              "elasticmapreduce:CreateStudioPresignedUrl",
              "elasticmapreduce:DescribeStudio",
              "elasticmapreduce:CreateStudio",
              "elasticmapreduce:ListStudios",
              "emr-serverless:ListApplications",
              "iam:ListRoles",
            ],
            resources: ["*"],
          }),
          new iam.PolicyStatement({
            sid: "EMRServerlessActions",
            effect: iam.Effect.ALLOW,
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
            resources: [emrs.attrArn, `${emrs.attrArn}/jobruns/*`],
          }),
          new iam.PolicyStatement({
            sid: "EMRServerlessVPCAccess",
            effect: iam.Effect.ALLOW,
            actions: ["ec2:CreateNetworkInterface"],
            resources: ["arn:aws:ec2:*:*:network-interface/*"],
            conditions: {
              StringEquals: {
                "aws:CalledViaLast": "ops.emr-serverless.amazonaws.com",
              },
            },
          }),
          new iam.PolicyStatement({
            sid: "EMRServerlessPassRole",
            effect: iam.Effect.ALLOW,
            actions: ["iam:PassRole"],
            resources: [jobRole.roleArn],
            conditions: { StringLike: { "iam:PassedToService": "emr-serverless.amazonaws.com" } },
          }),
        ],
      }),
    });
  }
}
