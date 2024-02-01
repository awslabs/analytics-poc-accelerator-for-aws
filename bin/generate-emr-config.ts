import * as fs from "fs";
import * as YAML from "yaml";
import { parse } from "ts-command-line-args";
import path = require("path");

const CDK_OUTPUT_FILE = "out.json";
const ASSETS_PATH = "assets";

type Example = "kinesis-printer" | "data-importer";

type Runtime = "ec2" | "eks" | "serverless";

interface CDKOutput {
  ODAPOCInfra: {
    dataBucket: string;
  };
  AnalyticsPOCStack: {
    emrClusterId: string;
    emrClusterJobRoleArn: string;

    emrOnEksJobExecutionRoleArn: string;
    emrOnEksVirtualClusterId: string;

    emrServerlessApplicationId: string;
    emrServerlessJobRoleArn: string;

    streamName: string;
    region: string;
  };
}

interface RuntimeConfig {
  job_role: string;
  cluster_id?: string;
  virtual_cluster_id?: string;
  application_id?: string;
}

interface ExampleConfig {
  s3_code_uri: string;
  spark_submit_opts: string;
  job_args: string;
  entry_point: string;

  show_stdout?: boolean;
  s3_logs_uri?: string;
}

class ConfigBuilder {
  cdkConfig: CDKOutput;
  emrConfigPath: string;
  runtimeConfig: RuntimeConfig;
  exampleConfig: ExampleConfig;

  constructor(cdk_output_file: string, example: Example, runtime: Runtime) {
    this.cdkConfig = JSON.parse(readFile(cdk_output_file)) as CDKOutput;

    if (example === "kinesis-printer") {
      this.exampleConfig = {
        s3_code_uri: `s3://${this.cdkConfig.ODAPOCInfra.dataBucket}/code/kprinter/`,
        spark_submit_opts: `--jars s3://${this.cdkConfig.ODAPOCInfra.dataBucket}/code/kinesis/jars/spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar`,
        job_args: `--stream_name,${this.cdkConfig.AnalyticsPOCStack.streamName},--stream_region,${this.cdkConfig.AnalyticsPOCStack.region},--checkpoint_s3_uri,s3://${this.cdkConfig.ODAPOCInfra.dataBucket}/logs/kprinter/`,
        entry_point: "entrypoint.py",
      };
      this.emrConfigPath = path.join(ASSETS_PATH, "kinesis-printer", ".emr");
    } else if (example === "data-importer") {
      this.exampleConfig = {
        s3_code_uri: `s3://${this.cdkConfig.ODAPOCInfra.dataBucket}/code/data-importer/`,
        spark_submit_opts: [
          `--jars ${(runtime === "eks") ? "local://" : ""}/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar`,
          `--conf spark.sql.catalog.dev.warehouse=s3://${this.cdkConfig.ODAPOCInfra.dataBucket}/output/poc/iceberg/`,
          "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
          "--conf spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog",
          "--conf spark.sql.catalog.dev.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
          " --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        ].join(" "),
        job_args: `s3://noaa-gsod-pds/20*/72793524234.csv,csv,poc_default.noaa_berg_seattle,years(DATE)`,
        entry_point: "penguin.py",
        s3_logs_uri: `s3://${this.cdkConfig.ODAPOCInfra.dataBucket}/logs/emr-${runtime}/`,
        show_stdout: true,
      };
      this.emrConfigPath = path.join(ASSETS_PATH, "data-importer", ".emr");
    } else {
      throw new Error(
        "Invalid example specified, only kinesis-printer is supported."
      );
    }

    if (runtime === "serverless") {
      this.runtimeConfig = {
        job_role: this.cdkConfig.AnalyticsPOCStack.emrServerlessJobRoleArn,
        application_id:
          this.cdkConfig.AnalyticsPOCStack.emrServerlessApplicationId,
      };
    } else if (runtime === "eks") {
      this.runtimeConfig = {
        job_role: this.cdkConfig.AnalyticsPOCStack.emrOnEksJobExecutionRoleArn,
        virtual_cluster_id:
          this.cdkConfig.AnalyticsPOCStack.emrOnEksVirtualClusterId,
      };
    } else if (runtime === "ec2") {
      this.runtimeConfig = {
        job_role: this.cdkConfig.AnalyticsPOCStack.emrClusterJobRoleArn,
        cluster_id: this.cdkConfig.AnalyticsPOCStack.emrClusterId,
      };
    } else {
      throw new Error(
        "Invalid runtime specified, only ec2 or serverless is supported."
      );
    }
  }

  write() {
    if (!fs.existsSync(this.emrConfigPath)) {
      fs.mkdirSync(this.emrConfigPath, { recursive: true });
    }
    fs.writeFileSync(
      path.join(this.emrConfigPath, "config.yaml"),
      YAML.stringify(this.buildConfig())
    );
  }

  baseConfig(): object {
    return {
      build: true,
      wait: true,
    };
  }

  buildConfig(): object {
    return {
      run: {
        ...this.baseConfig(),
        ...this.exampleConfig,
        ...this.runtimeConfig,
      },
    };
  }
}

interface IGenerateEmrConfigFlags {
  example: string;
  runtime: string;

  help?: boolean;
}

export const args = parse<IGenerateEmrConfigFlags>(
  {
    example: { type: String, typeLabel: "<kinesis-printer|data-importer>" },
    runtime: { type: String, typeLabel: "<ec2|eks|serverless>" },

    help: {
      type: Boolean,
      optional: true,
      alias: "h",
      description: "Prints this usage guide",
    },
  },
  {
    helpArg: "help",
    headerContentSections: [
      {
        header: "EMR Config Generator",
        content: "Generates an EMR CLI config.yaml file based on CDK outputs.",
      },
    ],
  }
);

// Create a function that tries to parse a json file and throws an error if it can't.
// It should take a filename as a parameter.
// It should return the content of the file.
function readFile(filename: string): string {
  try {
    return fs.readFileSync(filename, "utf-8");
  } catch (err) {
    throw new Error("File not found");
  }
}

new ConfigBuilder(
  CDK_OUTPUT_FILE,
  args.example as Example,
  args.runtime as Runtime
).write();
