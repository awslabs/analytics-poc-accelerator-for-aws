import crypto = require("crypto");
import { IResource, Resource, Stack } from "aws-cdk-lib";
import { Construct } from "constructs";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { aws_glue as glue } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { aws_lakeformation as lakeformation } from "aws-cdk-lib";

export const DEFAULT_DATABASE_NAME: string = "lf_default";

export interface ILakeFormation extends IResource {}

export class LakeFormation extends Resource implements ILakeFormation {
  readonly database: glue.CfnDatabase;

  constructor(scope: Construct, id: string) {
    super(scope, id);

    // Create a bucket to hold our data
    const lfBucket = new s3.Bucket(this, "LakeFormationBucket");

    // Lake Formation user-defined IAM role for registering locations
    // Required when using LF with EMR: https://docs.aws.amazon.com/lake-formation/latest/dg/registration-role.html
    const lfRegisterRole = new iam.Role(this, "aws-poc-lfregister-role", {
      assumedBy: new iam.CompositePrincipal(
        new iam.ServicePrincipal("glue.amazonaws.com"),
        new iam.ServicePrincipal("lakeformation.amazonaws.com")
      ),
      managedPolicies: [
        // iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonS3ReadOnlyAccess"),
      ],
      inlinePolicies: {
        athenaDefaultSparkPolicy: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                "s3:PutObject",
                "s3:ListBucket",
                "s3:DeleteObject",
                "s3:GetObject",
              ],
              resources: [lfBucket.bucketArn, lfBucket.arnForObjects("*")],
            }),
            new iam.PolicyStatement({
              actions: [
                "logs:CreateLogStream",
                "logs:DescribeLogStreams",
                "logs:CreateLogGroup",
                "logs:PutLogEvents",
              ],
              resources: [
                `arn:aws:logs:${Stack.of(this).region}:${Stack.of(this).account}:log-group:/aws-lakeformation-acceleration/*`,
                `arn:aws:logs:${Stack.of(this).region}:${Stack.of(this).account}:log-group:/aws-lakeformation-acceleration/*:log-stream:*`,
              ],
            }),
            new iam.PolicyStatement({
              actions: ["logs:DescribeLogGroups"],
              resources: [
                `arn:aws:logs:${Stack.of(this).region}:${Stack.of(this).account}:log-group::*`,
              ],
            }),
            // new iam.PolicyStatement({
            //     actions: ["cloudwatch:PutMetricData"],
            //     resources: ["*"],
            //     conditions: {
            //     StringEquals: {
            //         "cloudwatch:namespace": "AmazonAthenaForApacheSpark",
            //     },
            //     },
            // }),
          ],
        }),
      },
    });

    // Register our lake formation data source with LF
    const lfResource = new lakeformation.CfnResource(
      this,
      "LakeFormationDataBucketResource",
      {
        resourceArn: lfBucket.bucketArn,
        useServiceLinkedRole: false,
        roleArn: lfRegisterRole.roleArn,
      }
    );

    // In order to create a new database with LF-only permissions,
    // the user creating that table (or removing the IAM permissions)
    // needs to be a Lake Formation admin.
    // We can create the table with the Glue default and remove it with a Lambda.
    // Or we can create the table with empty default table permissions.
    // For either of these, the user making the call needs to be an LF admin.
    // The simplest approach is to make the cloudformation execution role a LF admin.
    // Alternatively, we can scope it to a specific role, but then it's a 2-step creation process:
    // - Create the database with the default
    // - Revoke the IAM permissions with a custom resource
    const dataLakeSettings = new lakeformation.CfnDataLakeSettings(
      this,
      "DataLakeSettings",
      {
        admins: [
          {
            dataLakePrincipalIdentifier: `arn:aws:iam::${Stack.of(this).account}:role/cdk-hnb659fds-cfn-exec-role-${Stack.of(this).account}-${Stack.of(this).region}`,
          },
        ],
      }
    );

    this.database = new glue.CfnDatabase(this, "LakeFormationDatabase", {
      catalogId: Stack.of(this).account,
      databaseInput: {
        name: DEFAULT_DATABASE_NAME,
        locationUri: lfBucket.s3UrlForObject(),
        createTableDefaultPermissions: [],
      },
    });
    this.database.addDependency(dataLakeSettings);

    // Allow the lambda function `CREATE_DATABASE` permissions
    // In order to do this, we need to:
    // 1. Create the role ahead of time
    // 2. Grant CREATE_DATABASE permissions with Lake Formation
    // 3. Use the role for the custom resource below
    // const lambdaRole = new iam.Role(this, "LambaRole", {
    //   assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
    //   managedPolicies: [
    //     iam.ManagedPolicy.fromAwsManagedPolicyName(
    //       "service-role/AWSLambdaBasicExecutionRole"
    //     ),
    //   ],
    //   inlinePolicies: this.buildLambdaStatements(lfBucket.bucketArn),
    // });

    // new lakeformation.CfnPrincipalPermissions(
    //   this,
    //   "CreateDatabasePermissions",
    //   {
    //     permissions: ["CREATE_DATABASE"],
    //     permissionsWithGrantOption: [],
    //     principal: {
    //       dataLakePrincipalIdentifier: lambdaRole.roleArn,
    //     },
    //     resource: {
    //       catalog: {},
    //     },
    //   }
    // );

    // Finally, since we have a dual-stack IAM/Lake Formation setup, we remove Super access to this new data catalog
    // Insufficient Glue permissions to access database lf_default :/
    // new cr.AwsCustomResource(this, "RemoveDatabaseIAM", {
    //   onUpdate: {
    //     service: "LakeFormation",
    //     action: "batchRevokePermissions",
    //     parameters: {
    //       Entries: [
    //         {
    //           Id: "1",
    //           Principal: {
    //             DataLakePrincipalIdentifier: "IAM_ALLOWED_PRINCIPALS",
    //           },
    //           Resource: { Database: { Name: lfDataCatalog.databaseName } },
    //           Permissions: ["ALL"],
    //         },
    //       ],
    //     },
    //     physicalResourceId: cr.PhysicalResourceId.of(
    //       "lf_delete_super_permissions"
    //     ),
    //   },
    //   role: lambdaRole,
    //   //   policy: cr.AwsCustomResourcePolicy.fromStatements(
    //   //     this.buildLambdaStatements(lfBucket.bucketArn)
    //   //   ),
    //   //   policy: cr.AwsCustomResourcePolicy.fromSdkCalls({
    //   //     resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE,
    //   //   }),
    // });
  }

  buildLambdaStatements(bucketArn: string): {
    [name: string]: iam.PolicyDocument;
  } {
    const gluePolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            "glue:CreateDatabase",
            "glue:UpdateDatabase",
            "glue:DeleteDatabase",
            "glue:GetDatabase",
            "glue:GetDatabases",
            "glue:CreateTable",
            "glue:UpdateTable",
            "glue:BatchDeleteTable",
            "glue:DeleteTable",
            "glue:GetTable",
            "glue:GetTables",
            "glue:GetTableVersions",
            "glue:SearchTables",
            "glue:BatchDeleteTableVersion",
          ],
          resources: [
            "arn:aws:glue:*:*:catalog",
            "arn:aws:glue:*:*:database/*",
            "arn:aws:glue:*:*:table/*",
            "arn:aws:glue:*:*:userDefinedFunction/*",
          ],
        }),
      ],
    });

    const lakeFormationPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            "lakeformation:GetDataAccess",
            "lakeformation:AddLFTagsToResource",
            "lakeformation:RemoveLFTagsFromResource",
            "lakeformation:GrantPermissions",
            "lakeformation:BatchGrantPermissions",
            "lakeformation:RevokePermissions",
            "lakeformation:BatchRevokePermissions",
            "lakeformation:ListPermissions",
            "lakeformation:CreateLFTag",
            "lakeformation:DeleteLFTag",
            "lakeformation:UpdateLFTag",
            "lakeformation:ListLFTags",
            "lakeformation:GetLFTag",
            "lakeformation:GetResourceLFTags",
            "lakeformation:RegisterResource",
            "lakeformation:SearchTablesByLFTags",
            "lakeformation:SearchDatabasesByLFTags",
          ],
          resources: ["*"],
        }),
      ],
    });

    const s3Policy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: ["s3:Get*", "s3:List*"],
          resources: [`${bucketArn}`, `${bucketArn}/*`],
        }),
      ],
    });

    return {
      gluePolicy,
      lakeFormationPolicy,
      s3Policy,
    };
  }

  public grantGlue(grantee: iam.IGrantable): iam.Grant {
    const glueGrant = iam.Grant.addToPrincipal({
      grantee,
      actions: [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:SearchTables",
        "glue:GetTable*",
        "glue:GetPartition*",
      ],
      resourceArns: [
        this.stack.formatArn({
          service: "glue",
          resource: "catalog",
        }),
        this.stack.formatArn({
          service: "glue",
          resource: "database",
          resourceName: `${this.database.ref}`,
        }),
        this.stack.formatArn({
          service: "glue",
          resource: "table",
          resourceName: `${this.database.ref}`,
        }),
        this.stack.formatArn({
          service: "glue",
          resource: "table",
          resourceName: `${this.database.ref}/*`,
        }),
      ],
    });

    const lfGrant = iam.Grant.addToPrincipal({
      grantee,
      actions: [
        "lakeformation:GetDataAccess",
        "lakeformation:GetResourceLFTags",
        "lakeformation:ListLFTags",
        "lakeformation:GetLFTag",
        "lakeformation:SearchTablesByLFTags",
        "lakeformation:SearchDatabasesByLFTags",
      ],
      resourceArns: ["*"],
    });

    return glueGrant.combine(lfGrant);
  }

  public grantWrite(principalArn: string) {
    new lakeformation.CfnPrincipalPermissions(
      this,
      `CreateDatabasePermissions-${crypto
        .createHash("sha1")
        .update(principalArn)
        .digest("hex")}`,
      {
        permissions: ["CREATE_TABLE", "ALTER", "DESCRIBE"],
        permissionsWithGrantOption: [],
        principal: {
          dataLakePrincipalIdentifier: principalArn,
        },
        resource: {
          database: {
            catalogId: Stack.of(this).account,
            name: DEFAULT_DATABASE_NAME,
          },
        },
      }
    );

    new lakeformation.CfnPrincipalPermissions(
      this,
      `CreateTablePermissions-${crypto
        .createHash("sha1")
        .update(principalArn)
        .digest("hex")}`,
      {
        permissions: ["ALL"],
        permissionsWithGrantOption: [],
        principal: {
          dataLakePrincipalIdentifier: principalArn,
        },
        resource: {
          table: {
            catalogId: Stack.of(this).account,
            databaseName: DEFAULT_DATABASE_NAME,
            tableWildcard: {},
          },
        },
      }
    );
  }
}
