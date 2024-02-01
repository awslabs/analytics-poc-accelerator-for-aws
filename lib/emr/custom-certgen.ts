import * as cdk from "aws-cdk-lib";
import {
  custom_resources as cr,
  aws_lambda as lambda,
  aws_s3 as s3,
} from "aws-cdk-lib";
import { Architecture } from "aws-cdk-lib/aws-lambda";
import { Construct } from "constructs";
import * as fs from "fs";
import path = require("path");

export interface SelfSignedCertificateProps {
  /**
   * Region to create the certificate for
   */
  region: string;

  /**
   * S3 bucket to store the generated certificate in
   */
  bucket: s3.IBucket;

  /**
   * Prefix to store the generate keyfile in
   */
  prefix: string;
}

export class SelfSignedCertificateResource extends Construct {
  public readonly s3Url: string;

  constructor(scope: Construct, id: string, props: SelfSignedCertificateProps) {
    super(scope, id);

    // const fn = new lambda.SingletonFunction(this, "Singleton", {
    //   uuid: "f7d4f730-4ee1-11e8-9c2d-fa7ae01bbebc",
    //   code: new lambda.InlineCode(
    //     fs.readFileSync("certgen-handler.py", { encoding: "utf-8" })
    //   ),
    //   handler: "index.main",
    //   timeout: cdk.Duration.seconds(300),
    //   runtime: lambda.Runtime.PYTHON_3_11,
    // });
    const fn2 = new lambda.SingletonFunction(this, "CertGen", {
      uuid: "39D89623-BF41-40C0-9612-C1161F9D2960",
      code: lambda.Code.fromAsset(path.join(__dirname, "certgen-handler"), {
        bundling: {
          // replacing lambda.Runtime.PYTHON_3_11.bundlingImage, due to https://github.com/jpadilla/pyjwt/issues/800
          image: cdk.DockerImage.fromRegistry(
            "public.ecr.aws/sam/build-python3.11:latest-x86_64"
          ),
          command: [
            "bash",
            "-c",
            "pip install cryptography -t /asset-output && cp -au . /asset-output",
          ],
          // Somehow still building for aarch64, need to figure that out. :)
          platform: Architecture.X86_64.dockerPlatform,
        },
      }),
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: "index.handler",
      architecture: lambda.Architecture.X86_64,
      timeout: cdk.Duration.seconds(10),
    });

    const provider = new cr.Provider(this, "Provider", {
      onEventHandler: fn2,
    });

    props.bucket.grantWrite(fn2, props.prefix + "*");
    props.bucket.grantDelete(fn2, props.prefix + "*");

    const resource = new cdk.CustomResource(this, "Resource", {
      serviceToken: provider.serviceToken,
      properties: {
        bucket: props.bucket.bucketName,
        prefix: props.prefix,
        region: props.region,
      },
    });

    this.s3Url = resource.getAttString("s3Url");
  }
}
