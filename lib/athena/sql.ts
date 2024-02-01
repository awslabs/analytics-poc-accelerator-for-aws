import { IResource, Resource } from "aws-cdk-lib";
import { aws_athena as athena } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { Construct } from "constructs";
import { WORKGROUP_READ_ACTIONS, WORKGROUP_RUN_QUERY_ACTIONS } from "./perms";

/**
 * Athena workgroup results location
 */
const WORKGROUP_RESULTS_PREFIX: string = "athena/results";

/**
 * Athena workgroup name
 */
const WORKGROUP_NAME: string = "poc_default";

export interface IAthenaSQL extends IResource {
  /**
   * The Athena workgroup name
   */
  readonly workgroupName: string;
}

export interface AthenaSQLProps {
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
}

export class AthenaSQL extends Resource implements IAthenaSQL {
  /**
   * The Athena workgroup
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

  constructor(scope: Construct, id: string, props: AthenaSQLProps) {
    super(scope, id);

    this.workgroupResultsPrefix =
      props.workgroupResultsPrefix?.replace(/\/$/, "") ??
      WORKGROUP_RESULTS_PREFIX;

    this.bucket =
      props.bucket ??
      new s3.Bucket(this, "Bucket", {
        versioned: true,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      });

    // Athena SQL Workgroup
    this.workgroup = new athena.CfnWorkGroup(this, "Workgroup", {
      name: WORKGROUP_NAME,
      recursiveDeleteOption: true,
      workGroupConfiguration: {
        engineVersion: {
          selectedEngineVersion: "Athena engine version 3",
        },
        resultConfiguration: {
          outputLocation: `s3://${this.bucket.bucketName}/${this.workgroupResultsPrefix}/`,
        },
      },
    });
  }

  /**
   * Grant read permissions for this Athena workgroup and it's contents to an IAM
   * principal (Role/Group/User).
   *
   * @param grantee The principal
   * @param objectsKeyPattern Restrict the permission to a certain key pattern (default '*')
   */
  public grantRead(grantee: iam.IGrantable, objectsKeyPattern: any = "*") {
    iam.Grant.addToPrincipal({
      grantee,
      resourceArns: [this.workgroup.ref],
      actions: WORKGROUP_READ_ACTIONS,
    });
  }

  /**
   * Grant execute permissions for this Athena workgroup to an IAM principal (Role/Group/User).
   *
   * @param grantee The principal
   * @param objectsKeyPattern Restrict the permission to a certain key pattern (default '*')
   */
  public grantExecute(grantee: iam.IGrantable) {
    iam.Grant.addToPrincipal({
      grantee,
      resourceArns: [
        this.stack.formatArn({
          service: "athena",
          resource: "workgroup",
          resourceName: this.workgroup.ref,
        }),
      ],
      actions: WORKGROUP_RUN_QUERY_ACTIONS,
    });
    iam.Grant.addToPrincipal({
      grantee,
      resourceArns: ["*"],
      actions: WORKGROUP_READ_ACTIONS,
    });

    // this.bucket.grantReadWrite(grantee, `${this.workgroupResultsPrefix}*`);
  }
}
