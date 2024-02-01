import { AssetHashType, DockerImage, Resource, Stack } from "aws-cdk-lib";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { aws_s3_deployment as s3deploy } from "aws-cdk-lib";
import { Construct } from "constructs";
import { aws_s3_assets as s3a } from "aws-cdk-lib";
import { Distribution } from "aws-cdk-lib/aws-cloudfront";
import { S3Origin } from "aws-cdk-lib/aws-cloudfront-origins";
import path = require("path");
import { readFileSync } from "fs";

export interface LandingPageProps {
  readonly bucket: s3.IBucket;
  readonly athenaWorkGroupName?: string;
  readonly emrStudioURL?: string;
  readonly emrServerlessApplicationID?: string;
}
export class LandingPage extends Resource {
  public readonly url: string;
  constructor(scope: Construct, id: string, props: LandingPageProps) {
    super(scope, id);

    // Build up our environment variables
    const env = {
      AWS_ACCOUNT_ID: Stack.of(this).account,
      ATHENA_WORKGROUP: props.athenaWorkGroupName ?? "",
      EMR_STUDIO_URL: props.emrStudioURL?.toString() ?? "",
      EMR_SERVERLESS_APP_ID: props.emrServerlessApplicationID ?? "",
    };

    const asset = new s3a.Asset(this, "LandingPage", {
      path: path.join(__dirname, "../../assets/ui/"),
      bundling: {
        image: DockerImage.fromRegistry("node:18"),
        command: [
          "bash",
          "-c",
          "npm ci && npx tailwindcss -i assets/css/tailwind.css -o /asset-output/assets/css/tailwind.css && npx @11ty/eleventy --output=/asset-output/ && rm -rf node_modules",
        ],
        environment: env,
        user: "root",
      },
      // maybe assetHash here? https://www.rehanvdm.com/blog/cdk-shorts-1-consistent-asset-hashing-nodejs
      // https://github.com/aws/aws-cdk/issues/9861
      assetHashType: AssetHashType.OUTPUT,
      // assetHash: "2023-09-01.001",
    });

    const distribution = new Distribution(this, "Distribution", {
      defaultBehavior: {
        origin: new S3Origin(props.bucket, {
          originPath: "website/",
        }),
      },
      defaultRootObject: "index.html",
    });
    this.url = `https://${distribution.distributionDomainName}`;

    const deployment = new s3deploy.BucketDeployment(this, "DeployWebsite", {
      sources: [
        s3deploy.Source.asset(path.join(__dirname, "../../assets/ui/_site/")),
        s3deploy.Source.data(
          "index.html",
          readFileSync("assets/ui/_site/index.html").toString()
        ),
      ],
      destinationBucket: props.bucket,
      destinationKeyPrefix: "website/",
      distribution: distribution,
    });
  }
}
