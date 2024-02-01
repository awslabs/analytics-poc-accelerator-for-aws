import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { aws_iam as iam } from "aws-cdk-lib";
import { aws_s3 as s3 } from "aws-cdk-lib";

export interface PermissionsStackProps extends cdk.StackProps {
  readonly bucket: s3.IBucket;
  readonly customerBucketName?: string;
}

export class PermissionsStack extends cdk.Stack {
  public readonly adminUser: iam.User;
  public readonly analystUser: iam.User;
  public readonly engineerUser: iam.User;

  public readonly defaultDataAccessPolicy: iam.ManagedPolicy;

  constructor(scope: Construct, id: string, props: PermissionsStackProps) {
    super(scope, id, props);

    // We create a set of pre-defined users and roles that are used throughout the
    // other stacks to grant access.
    // pocAdmin - Used to managed Lake Formation permissions
    // pocAnalyst - A limited user that primarily has access to run queries in Athena
    // pocEngineer - A power user that can run Spark jobs on EMR Serverless or EMR on EC2

    // First, create the users - they all share the same password for the POC.
    this.adminUser = new iam.User(this, "oda-poc-admin-user", {
      userName: "pocAdmin",
      password: cdk.SecretValue.unsafePlainText("This1IsATerriblePassword!"),
    });
    this.analystUser = new iam.User(this, "oda-poc-analyst-user", {
      userName: "pocAnalyst",
      password: cdk.SecretValue.unsafePlainText("This1IsATerriblePassword!"),
    });
    this.engineerUser = new iam.User(this, "oda-poc-engineer-user", {
      userName: "pocEngineer",
      password: cdk.SecretValue.unsafePlainText("This1IsATerriblePassword!"),
    });

    // This managed policy can be attached to job roles for easy access to data on S3 for GDC
    var userBucket: s3.IBucket | undefined;
    if (props.customerBucketName) {
      userBucket = s3.Bucket.fromBucketName(this, "oda-poc-user-bucket", props.customerBucketName);
    }
    this.defaultDataAccessPolicy = this._createDataAccessPolicy(props.bucket, userBucket);
  }

  /**
   * Create a managed policy that access to the following:
   * - User-provided bucket, in the same account, read access
   * - CDK bucket, read access to code/* and data/*
   * - CDK bucket, write access to output/* and logs/*(?)

   * @param cdkBucket 
   * @param userBucket 
   * @returns 
   */
  _createDataAccessPolicy(cdkBucket: s3.IBucket, userBucket?: s3.IBucket): iam.ManagedPolicy {
    const policy = new iam.ManagedPolicy(this, "oda-poc-data-access-policy");

    cdkBucket.grantRead(policy, "code/*");
    cdkBucket.grantRead(policy, "data/*");
    cdkBucket.grantReadWrite(policy, "output/*");
    cdkBucket.grantReadWrite(policy, "logs/*");

    if (userBucket) {
      userBucket.grantRead(policy);
    }

    return policy;
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
}
