import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { custom_resources as cr } from "aws-cdk-lib";
import { aws_glue as glue } from "aws-cdk-lib";
import { aws_lakeformation as lakeformation } from "aws-cdk-lib";
import * as glue_alpha from "@aws-cdk/aws-glue-alpha";

export interface DataCatalogStackProps extends cdk.StackProps {
  readonly dataCatalogName: string;
}

export class DataCatalogStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: DataCatalogStackProps) {
    super(scope, id, props);

    const dataCatalogName = props ? props.dataCatalogName : "default_poc";

    const myDatabase = new glue_alpha.Database(this, "SampleNOAADatabase", {
      databaseName: dataCatalogName,
    });

    const noaaTable = new glue_alpha.Table(this, "SampleNOAATable", {
      tableName: "noaa_gsod",
      database: myDatabase,
      columns: [
        { name: `station`, type: glue_alpha.Schema.STRING },
        { name: `date`, type: glue_alpha.Schema.STRING },
        { name: `latitude`, type: glue_alpha.Schema.STRING },
        { name: `longitude`, type: glue_alpha.Schema.STRING },
        { name: `elevation`, type: glue_alpha.Schema.STRING },
        { name: `name`, type: glue_alpha.Schema.STRING },
        { name: `temp`, type: glue_alpha.Schema.STRING },
        { name: `temp_attributes`, type: glue_alpha.Schema.STRING },
        { name: `dewp`, type: glue_alpha.Schema.STRING },
        { name: `dewp_attributes`, type: glue_alpha.Schema.STRING },
        { name: `slp`, type: glue_alpha.Schema.STRING },
        { name: `slp_attributes`, type: glue_alpha.Schema.STRING },
        { name: `stp`, type: glue_alpha.Schema.STRING },
        { name: `stp_attributes`, type: glue_alpha.Schema.STRING },
        { name: `visib`, type: glue_alpha.Schema.STRING },
        { name: `visib_attributes`, type: glue_alpha.Schema.STRING },
        { name: `wdsp`, type: glue_alpha.Schema.STRING },
        { name: `wdsp_attributes`, type: glue_alpha.Schema.STRING },
        { name: `mxspd`, type: glue_alpha.Schema.STRING },
        { name: `gust`, type: glue_alpha.Schema.STRING },
        { name: `max`, type: glue_alpha.Schema.STRING },
        { name: `max_attributes`, type: glue_alpha.Schema.STRING },
        { name: `min`, type: glue_alpha.Schema.STRING },
        { name: `min_attributes`, type: glue_alpha.Schema.STRING },
        { name: `prcp`, type: glue_alpha.Schema.STRING },
        { name: `prcp_attributes`, type: glue_alpha.Schema.STRING },
        { name: `sndp`, type: glue_alpha.Schema.STRING },
        { name: `frshtt`, type: glue_alpha.Schema.STRING },
      ],
      partitionKeys: [
        {
          name: "year",
          type: glue_alpha.Schema.STRING,
          comment: "year of the observation",
        },
      ],
      dataFormat: glue_alpha.DataFormat.CSV,
      bucket: s3.Bucket.fromBucketName(
        this,
        "SampleNOAABucket",
        "noaa-gsod-pds"
      ),
      // encryption: glue_alpha.TableEncryption.UNENCRYPTED,
    });

    const cfnTable = noaaTable.node.defaultChild as glue.CfnTable;
    const tableInput = cfnTable.tableInput as glue.CfnTable.TableInputProperty;
    const storageDescriptor =
      tableInput.storageDescriptor as glue.CfnTable.StorageDescriptorProperty;
    const serdeInfo =
      storageDescriptor.serdeInfo as glue.CfnTable.SerdeInfoProperty;

    tableInput.parameters["skip.header.line.count"] = "1";
    tableInput.parameters["has_encrypted_data"] = "false";

    // This is necessary for SHOW CREATE TABLE in Athena :)
    // @ts-ignore - readonly
    serdeInfo.parameters = {};

    // Add a few partitions manually
    new glue.CfnPartition(this, "NOAA2023Partition", {
      catalogId: this.account,
      databaseName: myDatabase.databaseName,
      tableName: noaaTable.tableName,
      partitionInput: {
        values: ["2023"],
        storageDescriptor: {
          location: "s3://noaa-gsod-pds/2023/",
          inputFormat: "org.apache.hadoop.mapred.TextInputFormat",
          outputFormat:
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
          serdeInfo: {
            serializationLibrary: "org.apache.hadoop.hive.serde2.OpenCSVSerde",
          },
        },
      },
    });
  }
}

export interface LakeFormationStackProps extends cdk.StackProps {
  readonly bucket: s3.IBucket;
  readonly athenaRole: iam.Role;
}

export class LakeFormationStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: LakeFormationStackProps) {
    super(scope, id, props);

    // We create a second data catalog that's restricted to LF permissions
    const lfDataCatalog = new DataCatalogStack(this, "lf-data-catalog", {
      dataCatalogName: "lakeformation_poc",
    });

    // Bucket with sample data
    const noaaBucket = s3.Bucket.fromBucketName(
      this,
      "noaa-bucket",
      "noaa-gsod-pds"
    );

    // Lake Formation user-defined IAM role for registering locations
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
              resources: [
                props.bucket.bucketArn,
                props.bucket.arnForObjects("*"),
              ],
            }),
            new iam.PolicyStatement({
              actions: ["s3:ListBucket", "s3:GetObject"],
              resources: [noaaBucket.bucketArn, noaaBucket.arnForObjects("*")],
            }),
            new iam.PolicyStatement({
              actions: [
                "logs:CreateLogStream",
                "logs:DescribeLogStreams",
                "logs:CreateLogGroup",
                "logs:PutLogEvents",
              ],
              resources: [
                `arn:aws:logs:${this.region}:${this.account}:log-group:/aws-lakeformation-acceleration/*`,
                `arn:aws:logs:${this.region}:${this.account}:log-group:/aws-lakeformation-acceleration/*:log-stream:*`,
              ],
            }),
            new iam.PolicyStatement({
              actions: ["logs:DescribeLogGroups"],
              resources: [
                `arn:aws:logs:${this.region}:${this.account}:log-group::*`,
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
          ],
        }),
      },
    });

    // We create a Lake Formation Admin and a Lake Formation user
    const lfAdmin = new iam.User(this, "aws-poc-lakeformation-admin", {
      userName: "datalake-admin",
      password: cdk.SecretValue.unsafePlainText("This1IsATerriblePassword!"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("AWSLakeFormationDataAdmin"),
        iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonAthenaFullAccess"),
      ],
    });
    lfAdmin.attachInlinePolicy(
      new iam.Policy(this, "lake-formation-slr", {
        statements: [
          new iam.PolicyStatement({
            actions: ["iam:CreateServiceLinkedRole"],
            resources: ["*"],
            conditions: {
              StringEquals: {
                "iam:AWSServiceName": "lakeformation.amazonaws.com",
              },
            },
          }),

          new iam.PolicyStatement({
            actions: ["iam:PutRolePolicy"],
            resources: [
              `arn:aws:iam::${this.account}:role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess`,
            ],
          }),
        ],
      })
    );

    const lfUser = new iam.User(this, "aws-poc-lakeformation-user", {
      userName: "datalake-analyst",
      password: cdk.SecretValue.unsafePlainText(
        "This1IsAlsoATerriblePassword!"
      ),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonAthenaFullAccess"),
      ],
    });

    // The user needs access to read and write query results
    props.bucket.grantReadWrite(lfUser, "athena/results/sql/*");

    // Now we try to register our example data source with LF
    const lfResource = new lakeformation.CfnResource(
      this,
      "aws-poc-lakeformation-noaa-resource",
      {
        resourceArn: noaaBucket.bucketArn,
        useServiceLinkedRole: false,

        // the properties below are optional
        roleArn: lfRegisterRole.roleArn,
      }
    );

    // Finally, since we have a dual-stack IAM/Lake Formation setup, we remove Super access to this new data catalog
    new cr.AwsCustomResource(this, "RemoveDatabaseIAM", {
      onCreate: {
        service: "LakeFormation",
        action: "batchRevokePermissions",
        parameters: {
          Entries: [
            {
              Id: "1",
              Principal: {
                DataLakePrincipalIdentifier: "IAM_ALLOWED_PRINCIPALS",
              },
              Resource: { Database: { Name: "lfDataCatalog" } },
              Permissions: ["ALL"],
            },
            {
              Id: "2",
              Principal: {
                DataLakePrincipalIdentifier: "IAM_ALLOWED_PRINCIPALS",
              },
              Resource: {
                Table: { DatabaseName: "lfDataCatalog", Name: "noaa_gsod" },
              },
              Permissions: ["ALL"],
            },
          ],
        },
        physicalResourceId: cr.PhysicalResourceId.of(
          "lf_delete_super_permissions"
        ),
      },
      policy: cr.AwsCustomResourcePolicy.fromSdkCalls({
        resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE,
      }),
    });

    // Now we try to grant our Athena Spark role/workgroup user access to the database.
    // Example code for when LF is supported on Athena Spark
    // new lakeformation.CfnPrincipalPermissions(this, "athena-grant", {
    //   principal: {dataLakePrincipalIdentifier: props.athenaRole.roleArn},
    //   // permissions: ["SELECT", "DESCRIBE", "CREATE_TABLE", "DATA_LOCATION_ACCESS"],
    //   permissions: ["DESCRIBE", "CREATE_TABLE",],
    //   permissionsWithGrantOption: [],
    //   resource: {
    //     database: {
    //       catalogId: this.account,
    //       name: "poc_default"
    //     }
    //   }
    // })

    new cdk.CfnOutput(this, "lakeFormationRegisterRole", {
      value: lfRegisterRole.roleArn,
      description: "The role used to register Lake Formation locations",
      exportName: "lakeFormationRegisterRole",
    });
  }
}
