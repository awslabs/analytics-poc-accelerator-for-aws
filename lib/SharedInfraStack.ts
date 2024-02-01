import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { aws_ec2 as ec2 } from "aws-cdk-lib";

/**
 * Shared infrastructure -- VPC and S3 Bucket
 */

export class SharedInfraStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;
  public readonly bucket: s3.Bucket;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    this.vpc = new ec2.Vpc(this, "Vpc", { maxAzs: 2 });

    // EMR/EMR Studio require VPCs to be tagged appropriately
    // https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-managed-iam-policies.html
    cdk.Tags.of(this.vpc).add(
      "for-use-with-amazon-emr-managed-policies",
      "true"
    );

    this.bucket = new s3.Bucket(this, "aws-poc-data-bucket", {
      versioned: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });

    new cdk.CfnOutput(this, "dataBucket", {
      value: this.bucket.bucketName,
      description: "S3_BUCKET",
    });
  }
}
