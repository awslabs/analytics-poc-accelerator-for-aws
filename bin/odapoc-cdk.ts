#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { AnalyticsPOCStack } from "../lib/AnalyticsPOCStack";
import { SharedInfraStack } from "../lib/SharedInfraStack";

const app = new cdk.App();

const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

// Some basic requirements like a VPC and S3 bucket
const infra = new SharedInfraStack(app, "ODAPOCInfra", { env });

// For the purpose of the PoC, we create a set of users
// You can, in the future, import your own users by passing in their usernames or arns.
// const adminPrincipals = ["admin_user"]

// We allow the user to provide data sources they want their POC stack to have access to
const kinesisarns = app.node.tryGetContext("kinesisarns")?.split(",");

// Allow the user to specify different environments on the command-line - ec2,serverless,eks
const emrEnvironments = (
  app.node.tryGetContext("emrdeployments") ?? "serverless"
).split(",");

const poc = new AnalyticsPOCStack(app, "AnalyticsPOCStack", {
  vpc: infra.vpc,
  bucket: infra.bucket,

  createEMRCluster: emrEnvironments.includes("ec2"),
  createEMRApplication: emrEnvironments.includes("serverless"),
  createEMROnEks: emrEnvironments.includes("eks"),
  createEMRStudio: false,

  createAthenaSQL: false,
  createAthenaSpark: false,

  // adminPrincipals can administer user permissions and have full read/write access to all resources
  // dataEngineerPrincipals can read/write all tables created as part of the POC
  // dataAnalystPrincipals can read/query all tables created as part of the POC
  dataSources: kinesisarns,

  env,
  
  //Provided by the user
  //example "arn:aws:iam::0123456788912:role/ROLE-NAME"
  eksAdminRoleArn: "arn:aws:iam::568026268536:role/Admin",

  //Provided by the user
  //For testing purpose you can set it to "0.0.0.0/0"
  publicAccessCIDRs:  ["0.0.0.0/0"]
});
