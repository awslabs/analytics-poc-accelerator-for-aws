import { IResource, Resource, Stack } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { Construct } from "constructs";
import { aws_glue as glue } from "aws-cdk-lib";
import * as glue_alpha from "@aws-cdk/aws-glue-alpha";

export const DEFAULT_DATABASE_NAME: string = "poc_default";

export interface ISampleData extends IResource {}

export interface SampleDataProps {
  readonly lakeFormationEnabled?: boolean;
  readonly databaseName?: string;
}

export class SampleData extends Resource implements ISampleData {
  public readonly database: glue_alpha.IDatabase;
  public readonly table: glue_alpha.Table;

  constructor(scope: Construct, id: string, props: SampleDataProps) {
    super(scope, id);

    const databaseName = props.databaseName ?? DEFAULT_DATABASE_NAME;

    // TODO: Take a database as a parameter - we don't want to grantWrite to this sample data...
    this.database = new glue_alpha.Database(this, "SampleNOAADatabase", {
      databaseName: databaseName,
    });

    const noaaTable = new glue_alpha.Table(this, "SampleNOAATable", {
      tableName: "noaa_gsod",
      database: this.database,
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
    this.table = noaaTable;

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
    ["2021", "2022", "2023"].forEach((year) => {
      new glue.CfnPartition(this, `NOAA${year}Partition`, {
        catalogId: Stack.of(this).account,
        databaseName: this.database.databaseName,
        tableName: noaaTable.tableName,
        partitionInput: {
          values: [year],
          storageDescriptor: {
            location: `s3://noaa-gsod-pds/${year}/`,
            inputFormat: "org.apache.hadoop.mapred.TextInputFormat",
            outputFormat:
              "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            serdeInfo: {
              serializationLibrary:
                "org.apache.hadoop.hive.serde2.OpenCSVSerde",
            },
          },
        },
      });
    });
  }

  public grantRead(grantee: iam.IGrantable) {
    iam.Grant.addToPrincipal({
      grantee,
      actions: [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:SearchTables",
        "glue:GetTable*",
        "glue:GetPartition*",
      ],
      resourceArns: [
        this.database.catalogArn,
        this.database.databaseArn,
        this.table.tableArn,
        this.stack.formatArn({
          service: "glue",
          resource: "table",
          resourceName: `${this.database.databaseName}/*`,
        }),
      ],
    });
    this.table.grantRead(grantee);
  }

  public grantWrite(grantee: iam.IGrantable) {
    iam.Grant.addToPrincipal({
      grantee,
      actions: ["glue:Create*", "glue:Update*"],
      resourceArns: [
        this.database.catalogArn,
        this.database.databaseArn,
        this.stack.formatArn({
          service: "glue",
          resource: "table",
          resourceName: `${this.database.databaseName}/*`,
        }),
      ],
    });
  }
}
