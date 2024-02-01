import { Stack, aws_athena as athena } from "aws-cdk-lib";
import { IResource, Resource } from "aws-cdk-lib";
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { Asset } from "aws-cdk-lib/aws-s3-assets";
import path = require("path");
import * as fs from "fs";
import { custom_resources as cr } from "aws-cdk-lib";

/**
 * Athena workgroup results location
 */
const WORKGROUP_RESULTS_PREFIX: string = "athena/results/spark";

/**
 * Athena Spark workgroup name
 */
const SPARK_WORKGROUP_NAME: string = "poc_spark";

export interface IAthenaSpark extends IResource, iam.IGrantable {
  /**
   * The Athena Spark workgroup
   */
  readonly workgroup: athena.CfnWorkGroup;

  /**
   * The Athena Spark workgroup name
   */
  readonly workgroupName: string;
}

export interface AthenaSparkProps {
  /**
   * S3 bucket for storage of Athena results
   *
   * @default - A bucket will automatically be created, accessed via the `bucket` property
   */
  readonly bucket?: s3.IBucket;

  /**
   * Prefix where notebooks are saved in the S3 bucket
   *
   * @default - athena/results/
   */
  readonly workgroupResultsPrefix?: string;

  /**
   * The name of the workgroup
   *
   * @default - poc_default
   */
  readonly workgroupName?: string;

  /**
   * An IAM role to use when running jobs on the application.
   *
   * @default - A role will automatically be created, it can be accessed via the `jobRole` property
   */
  readonly jobRole?: iam.IRole;

  /**
   * Creates a sample notebook for querying data with Spark in Athena.
   *
   * @default - false
   */
  readonly createSampleNotebook?: boolean;
}

export class AthenaSpark extends Resource implements IAthenaSpark {
  /**
   * The Athena Spark workgroup
   */
  public readonly workgroup: athena.CfnWorkGroup;

  /**
   * The Athena workgroup name
   */
  public readonly workgroupName: string;

  /**
   * The S3 bucket used for results storage.
   */
  public readonly bucket: s3.IBucket;

  /**
   * Prefix where notebooks are saved in the S3 bucket
   */
  public readonly workgroupResultsPrefix: string;

  /**
   * The IAM role that can be used by the notebook.
   */
  public readonly jobRole: iam.IRole;

  /**
   * The principal to grant permissions to.
   */
  public readonly grantPrincipal: iam.IPrincipal;

  constructor(scope: Construct, id: string, props: AthenaSparkProps) {
    super(scope, id);

    this.workgroupResultsPrefix =
      props.workgroupResultsPrefix?.replace(/\/$/, "") ??
      WORKGROUP_RESULTS_PREFIX;

    this.workgroupName = props.workgroupName ?? SPARK_WORKGROUP_NAME;

    this.bucket =
      props.bucket ??
      new s3.Bucket(this, "Bucket", {
        versioned: true,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      });

    this.jobRole =
      props.jobRole ??
      new iam.Role(this, "JobRole", {
        assumedBy: new iam.CompositePrincipal(
          new iam.ServicePrincipal("athena.amazonaws.com"),
          new iam.AccountPrincipal(Stack.of(this).account).withConditions({
            StringEquals: { "aws:SourceAccount": Stack.of(this).account },
            ArnLike: {
              "aws:SourceArn": `arn:aws:athena:${Stack.of(this).region}:${Stack.of(this).account}:workgroup/${this.workgroupName}`,
            },
          })
        ),
        inlinePolicies: {
          athenaDefaultSparkPolicy: this.generateDefaultPolicy(),
        },
      });
    this.grantPrincipal = this.jobRole;

    this.workgroup = new athena.CfnWorkGroup(this, "SparkWorkgroup", {
      name: this.workgroupName,
      recursiveDeleteOption: true,
      workGroupConfiguration: {
        engineVersion: {
          selectedEngineVersion: "PySpark engine version 3",
        },
        resultConfiguration: {
          outputLocation: `s3://${this.bucket.bucketName}/${this.workgroupResultsPrefix}//`,
        },
        executionRole: this.jobRole.roleArn,
      },
    });

    if (props.createSampleNotebook) {
      const fileAsset = new Asset(this, "SampleSingleFileAsset", {
        path: path.join(__dirname, "..", "..", "assets", "import.ipynb"),
      });
      const athenaNotebook = new cr.AwsCustomResource(this, "AthenaNotebook", {
        onCreate: {
          service: "Athena",
          action: "importNotebook",
          parameters: {
            Name: "ImportData",
            Type: "IPYNB",
            WorkGroup: this.workgroupName,
            Payload: fs.readFileSync(
              path.join(__dirname, "..", "assets", "import.ipynb"),
              "utf-8"
            ),
          },
          physicalResourceId: cr.PhysicalResourceId.fromResponse("NotebookId"),
        },
        policy: cr.AwsCustomResourcePolicy.fromSdkCalls({
          resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE,
        }),
      });
      const athenaNotebookDelete = new cr.AwsCustomResource(
        this,
        "NotebookDelete",
        {
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
        }
      );
    }
  }

  /**
   * Grant execute permissions for this Athena workgroup to an IAM principal (Role/Group/User).
   *
   * @param grantee The principal
   * @param objectsKeyPattern Restrict the permission to a certain key pattern (default '*')
   */
  public grantExecute(grantee: iam.IGrantable): iam.Grant {
    return iam.Grant.addToPrincipal({
      grantee,
      resourceArns: [
        this.stack.formatArn({
          service: "athena",
          resource: "workgroup",
          resourceName: this.workgroup.ref,
        }),
      ],
      actions: [
        "athena:GetWorkGroup",
        "athena:ListNotebook*",
        "athena:CreateNotebook",
      ],
    });
  }

  generateDefaultPolicy(): cdk.aws_iam.PolicyDocument {
    return new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          actions: [
            "s3:PutObject",
            "s3:ListBucket",
            "s3:DeleteObject",
            "s3:GetObject",
          ],
          resources: [this.bucket.bucketArn, this.bucket.arnForObjects("*")],
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
          resources: [
            `arn:aws:athena:${Stack.of(this).region}:${Stack.of(this).account}:workgroup/${this.workgroupName}`,
          ],
        }),
        new iam.PolicyStatement({
          actions: [
            "logs:CreateLogStream",
            "logs:DescribeLogStreams",
            "logs:CreateLogGroup",
            "logs:PutLogEvents",
          ],
          resources: [
            `arn:aws:logs:${Stack.of(this).region}:${Stack.of(this).account}:log-group:/aws-athena:*`,
            `arn:aws:logs:${Stack.of(this).region}:${Stack.of(this).account}:log-group:/aws-athena*:log-stream:*`,
          ],
        }),
        new iam.PolicyStatement({
          actions: ["logs:DescribeLogGroups"],
          resources: [
            `arn:aws:logs:${Stack.of(this).region}:${Stack.of(this).account}:log-group::*`,
          ],
        }),
        new iam.PolicyStatement({
          actions: ["cloudwatch:PutMetricData"],
          resources: ["*"],
          conditions: {
            StringEquals: {
              "cloudwatch:namespace": "AmazonAthenaForApacheSpark",
            },
          },
        }),
        new iam.PolicyStatement({
          actions: ["lakeformation:GetDataAccess"],
          resources: ["*"],
        }),
      ],
    });
  }
}
