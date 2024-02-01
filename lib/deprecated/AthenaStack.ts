import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { aws_athena as athena } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { custom_resources as cr } from "aws-cdk-lib";
import { Asset } from "aws-cdk-lib/aws-s3-assets";
import path = require("path");
import * as fs from "fs";
import { PermissionsStack } from "./PermissionsStack";

export interface AthenaStackProps extends cdk.StackProps {
  readonly bucket: s3.IBucket;
  readonly permissions: PermissionsStack;
}

export class AthenaStack extends cdk.Stack {
  public readonly athenaWorkgroupRole: iam.Role;

  constructor(scope: Construct, id: string, props: AthenaStackProps) {
    super(scope, id, props);

    // Athena infra and resources
    const athenaSQLName = "sql-poc";
    const athenaSparkName = "spark-poc";

    // Bucket with sample data
    const noaaBucket = s3.Bucket.fromBucketName(this, "noaa-bucket", "noaa-gsod-pds");

    // Grant the Data Engineer user full Athena access
    props.permissions.engineerUser.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonAthenaFullAccess"));
    
    // Athena IAM role for both SQL and Spark workgroups
    this.athenaWorkgroupRole = new iam.Role(this, "aws-poc-athena-role", {
      assumedBy: new iam.CompositePrincipal(
        new iam.ServicePrincipal("athena.amazonaws.com"),
        new iam.AccountPrincipal(this.account).withConditions({
          StringEquals: { "aws:SourceAccount": this.account },
          ArnLike: { "aws:SourceArn": `arn:aws:athena:${this.region}:${this.account}:workgroup/${athenaSparkName}` },
        })
      ),
      managedPolicies: [
        // iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonS3ReadOnlyAccess"),
      ],
      inlinePolicies: {
        athenaDefaultSparkPolicy: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: ["s3:PutObject", "s3:ListBucket", "s3:DeleteObject", "s3:GetObject"],
              resources: [props.bucket.bucketArn, props.bucket.arnForObjects("*")],
            }),
            new iam.PolicyStatement({
              actions: ["s3:ListBucket", "s3:GetObject"],
              resources: [noaaBucket.bucketArn, noaaBucket.arnForObjects("*")],
            }),
            new iam.PolicyStatement({
              actions: [
                "athena:GetWorkGroup",
                "athena:TerminateSession",
                "athena:GetSession",
                "athena:GetSessionStatus",
                "athena:ListSessions",
                "athena:StartCalculationExecution",
                "athena:GetCalculationExecutionCode",
                "athena:StopCalculationExecution",
                "athena:ListCalculationExecutions",
                "athena:GetCalculationExecution",
                "athena:GetCalculationExecutionStatus",
                "athena:ListExecutors",
                "athena:ExportNotebook",
                "athena:UpdateNotebook",
              ],
              resources: [`arn:aws:athena:${this.region}:${this.account}:workgroup/${athenaSparkName}`],
            }),
            new iam.PolicyStatement({
              actions: ["logs:CreateLogStream", "logs:DescribeLogStreams", "logs:CreateLogGroup", "logs:PutLogEvents"],
              resources: [
                `arn:aws:logs:${this.region}:${this.account}:log-group:/aws-athena:*`,
                `arn:aws:logs:${this.region}:${this.account}:log-group:/aws-athena*:log-stream:*`,
              ],
            }),
            new iam.PolicyStatement({
              actions: ["logs:DescribeLogGroups"],
              resources: [`arn:aws:logs:${this.region}:${this.account}:log-group::*`],
            }),
            new iam.PolicyStatement({
              actions: ["cloudwatch:PutMetricData"],
              resources: ["*"],
              conditions: { StringEquals: { "cloudwatch:namespace": "AmazonAthenaForApacheSpark" } },
            }),
            new iam.PolicyStatement({
              actions: ["lakeformation:GetDataAccess"],
              resources: ["*"],
            }),
          ],
        }),
      },
    });

    // Athena SQL Workgroup
    const athenaSQL = new athena.CfnWorkGroup(this, "aws-poc-athena-sql-workgroup", {
      name: athenaSQLName,
      recursiveDeleteOption: true,
      workGroupConfiguration: {
        engineVersion: {
          selectedEngineVersion: "Athena engine version 3",
        },
        resultConfiguration: {
          outputLocation: `s3://${props.bucket.bucketName}/athena/results/sql/`,
        },
      },
    });

    // Athena Spark Workgroup
    const athenaSpark = new athena.CfnWorkGroup(this, "aws-poc-athena-spark-workgroup", {
      name: athenaSparkName,
      recursiveDeleteOption: true,
      workGroupConfiguration: {
        engineVersion: {
          selectedEngineVersion: "PySpark engine version 3",
        },
        resultConfiguration: {
          outputLocation: `s3://${props.bucket.bucketName}/athena/results/spark/`,
        },
        executionRole: this.athenaWorkgroupRole.roleArn,
      },
    });

    // Example Notebook
    const fileAsset = new Asset(this, "SampleSingleFileAsset", {
      path: path.join(__dirname, "..", "assets", "import.ipynb"),
    });
    const athenaNotebook = new cr.AwsCustomResource(this, "AthenaNotebook", {
      onCreate: {
        service: "Athena",
        action: "importNotebook",
        parameters: {
          Name: "ImportData",
          Type: "IPYNB",
          WorkGroup: athenaSparkName,
          Payload: fs.readFileSync(path.join(__dirname, "..", "assets", "import.ipynb"), "utf-8"),
        },
        physicalResourceId: cr.PhysicalResourceId.fromResponse("NotebookId"),
      },
      policy: cr.AwsCustomResourcePolicy.fromSdkCalls({
        resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE,
      }),
    });
    const athenaNotebookDelete = new cr.AwsCustomResource(this, "NotebookDelete", {
      onDelete: {
        service: "Athena",
        action: "deleteNotebook",
        parameters: {
          NotebookId: athenaNotebook.getResponseField("NotebookId"),
        },
      },
      policy: cr.AwsCustomResourcePolicy.fromSdkCalls({
        resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE,
      }),
    });
  }
}
