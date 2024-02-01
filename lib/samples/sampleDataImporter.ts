import path = require("path");
import {
  BundlingFileAccess,
  DockerImage,
  Resource,
  aws_s3 as s3,
  aws_s3_deployment as s3deploy,
} from "aws-cdk-lib";
import { Construct } from "constructs";

export interface SampleDataImporterProps {
  /**
   * The name of the S3 bucket to use for build artifacts
   *
   * @default - Creates a new bucket.
   */
  readonly artifactsBucket?: s3.IBucket;

  /**
   * The prefix to use for EMR artifacts.
   *
   * @default - code/
   */
  readonly artifactsBucketPrefix?: string;
}

export class SampleDataImporterApp extends Resource {
  /**
   * Uploads the relevant artifacts for our data importer app
   */

  /**
   * Location where artifacts are deployed
   */
  readonly bucket: s3.IBucket;
  readonly bucketPrefix: string;

  constructor(scope: Construct, id: string, props: SampleDataImporterProps) {
    super(scope, id);

    this.bucket = props.artifactsBucket ?? new s3.Bucket(this, "DataAppBucket");
    this.bucketPrefix = props.artifactsBucketPrefix ?? "code/data-importer/";

    new s3deploy.BucketDeployment(this, "DataImporterDeployment", {
      destinationBucket: this.bucket,
      destinationKeyPrefix: this.bucketPrefix,
    //   retainOnDelete: false,
      prune: false,
      sources: [
        s3deploy.Source.asset(
          path.join(__dirname, "../../assets/data-importer"),
          { exclude: [".emr"] }
        ),
      ],
    });
  }
}
