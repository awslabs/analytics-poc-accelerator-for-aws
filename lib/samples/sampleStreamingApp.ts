import path = require("path");
import {
  BundlingFileAccess,
  DockerImage,
  Resource,
  aws_s3 as s3,
  aws_s3_deployment as s3deploy,
} from "aws-cdk-lib";
import { Construct } from "constructs";

export interface SampleStreamingAppProps {
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

export class SampleStreamingApp extends Resource {
  /**
   * Uploads the relevant artifacts for a sample Spark streaming app
   */

  /**
   * Location where artifacts are deployed
   */
  readonly bucket: s3.IBucket;
  readonly bucketPrefix: string;

  constructor(scope: Construct, id: string, props: SampleStreamingAppProps) {
    super(scope, id);

    this.bucket =
      props.artifactsBucket ?? new s3.Bucket(this, "StreamingAppBucket");
    this.bucketPrefix = props.artifactsBucketPrefix ?? "code/kinesis/";

    new s3deploy.BucketDeployment(this, "StreamingAppDeployment", {
      destinationBucket: this.bucket,
      destinationKeyPrefix: this.bucketPrefix,
      memoryLimit: 1024,
      // retainOnDelete: false,
      prune: false,
      // extract: false,
      sources: [
        s3deploy.Source.asset(
          path.join(__dirname, "../../assets/code/spark-streaming/"),
          {
            bundling: {
              image: DockerImage.fromBuild(
                path.join(__dirname, "../../assets/code/spark-streaming/")
              ),
              command: [
                "/bin/bash",
                "-c",
                "mkdir /asset-output/jars/ && cp /build/spark-sql-kinesis-connector-1.0.0/target/spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar /asset-output/jars/",
              ],
            },
          }
        ),
      ],
    });
  }
}
