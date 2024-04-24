import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { aws_ec2 as ec2 } from "aws-cdk-lib";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { Cluster } from "./emr/cluster";
import { Application } from "./emr/serverless";
import { Studio } from "./emr/studio";
import { AthenaSQL } from "./athena/sql";
import { SampleData } from "./catalog/glue";
import { LandingPage } from "./ui/landing-page";
import { AthenaSpark } from "./athena/spark";
import { LakeFormation } from "./catalog/lakeFormation";
import { SampleStreamingApp } from "./samples/sampleStreamingApp";
import { processing } from "@cdklabs/aws-data-solutions-framework";
import { SampleDataImporterApp } from "./samples/sampleDataImporter";
import { VirtualCluster } from "./emr/containers";

export interface AnalyticsPOCStackProps extends cdk.StackProps {
  // Infrastructure properties for the POC Stack
  readonly vpc: ec2.IVpc;
  readonly bucket: s3.IBucket;

  // Options for configuring the POC Stack
  readonly createEMRCluster?: boolean;
  readonly createEMRApplication?: boolean;
  readonly createEMRStudio?: boolean;
  readonly createEMROnEks?: boolean;

  readonly createAthenaSQL?: boolean;
  readonly createAthenaSpark?: boolean;

  readonly createLakeFormationDatabase?: boolean;

  readonly adminPrincipals?: iam.IPrincipal[];
  readonly adminUsers?: iam.IUser[];
  readonly eksAdminRoleArn?: string;
  readonly publicAccessCIDRs?: string[];
  readonly createEmrOnEksServiceLinkedRole?: boolean;

  // Today, this is just a list of Kinesis ARNs that trigger a build of EMR artifacts
  // and grant read access to our applications.
  readonly dataSources?: [];
}

export class AnalyticsPOCStack extends cdk.Stack {
  readonly emr: Cluster;
  readonly emrApplication: Application;
  readonly emrVirtualCluster: VirtualCluster;
  readonly emrStudio: Studio;
  readonly emrRuntimeRole: iam.Role;
  readonly emrStudioServiceRole: iam.Role;
  readonly athenaSQL: AthenaSQL;
  readonly athenaSpark: AthenaSpark;

  constructor(scope: Construct, id: string, props: AnalyticsPOCStackProps) {
    super(scope, id, props);

    // If desired, grant permissions to an IAM user
    const adminPrincipals: iam.IPrincipal[] = []; // [iam.User.fromUserName(this, "AdminUser", "admin-user")];

    // Create a managed role for granting access, and then grant it to the adminPrincipals
    // This works, but I'm wondering if it's better to create managed policies per construct...
    const dataEngineerRole = new iam.ManagedPolicy(
      this,
      "DataEngineerRole",
      {}
    );
    // We apply this here (as opposed to `users` above) to avoid cyclic dependencies
    (
      adminPrincipals?.filter((x) => x instanceof iam.User) as iam.User[]
    ).forEach((x) => x.addManagedPolicy(dataEngineerRole));

    // Create a sample table for the POC
    // TODO: Create a sample database, then pass it to the SampleData
    const sampleData = new SampleData(this, "SampleData", {});
    sampleData.grantRead(dataEngineerRole);

    // And a sample data ingestor
    const dataImporter = new SampleDataImporterApp(this, "DataImporterApp", {
      artifactsBucket: props.bucket,
    });

    // So basically, we orchestrate all this here
    // 1. Create a set of users
    // 2. Create a set of data catalogs
    // 3. If requested, create an EMR cluster (default yes)
    // 4. If requested, create an EMR Studio (default yes)
    // 5. (future) If requested create an EMR Virtual Cluster
    // 6. Connect EMR Studio to either/and EC2 or EKS
    // 7. Create an EMR Serverless app
    // 8. Create Athena SQL and Spark workgroups
    // We deploy ONE STACK, which the user can configure w/different resources
    if (props.createEMRCluster) {
      this.emr = new Cluster(this, "EMRCluster", {
        vpc: props.vpc,
        loggingBucket: props.bucket,
        ssmSessionPermissions: true,
        runtimeRoleOptions: {
          createRuntimeRole: true,
          withStudioAccess: true,
        },
        artifactsBucket: props.bucket,
        releaseLabel: "emr-6.15.0" as processing.EmrRuntimeVersion,
      });
      if (this.emr.jobRole) {
        new cdk.CfnOutput(this, "emrClusterJobRoleArn", {
          value: this.emr.jobRole.roleArn,
          description: "RUNTIME_ROLE_ARN",
        });
      }
      new cdk.CfnOutput(this, "emrClusterId", {
        value: this.emr.clusterId,
        description: "CLUSTER_ID",
      });
      sampleData.grantRead(this.emr);
      sampleData.grantWrite(this.emr);
      props.bucket.grantReadWrite(this.emr);
      this.emr.grantConsoleAccess(dataEngineerRole);
    }

    if (props.createEMRApplication) {
      this.emrApplication = new Application(this, "EMRServerlessApp", {
        vpc: props.vpc,
        releaseLabel: "emr-6.15.0" as processing.EmrRuntimeVersion,
      });
      new cdk.CfnOutput(this, "emrServerlessJobRoleArn", {
        value: this.emrApplication.jobRole.roleArn,
        description: "JOB_ROLE_ARN",
      });
      new cdk.CfnOutput(this, "emrServerlessApplicationId", {
        value: this.emrApplication.applicationId,
        description: "APPLICATION_ID",
      });

      // With EMR Serverless, each job can have a different runtime role
      // We create that role in Application construct, but grant it the
      // necessary access to write logs and read/write data here.
      sampleData.grantRead(this.emrApplication);
      sampleData.grantWrite(this.emrApplication);
      props.bucket.grantRead(this.emrApplication);
      props.bucket.grantWrite(this.emrApplication, "logs/*");
      props.bucket.grantWrite(this.emrApplication, "output/*");
      this.emrApplication.grantManage(dataEngineerRole);
    }

    if (props.createEMROnEks) {
      this.emrVirtualCluster = new VirtualCluster(this, "EMRVirtualCluster", {
        vpc: props.vpc,
        adminRole: iam.Role.fromRoleArn(
          this,
          "eksAdminRole",
          props.eksAdminRoleArn!
        ),
        publicAccessCIDRs: props.publicAccessCIDRs!,
        createEmrOnEksServiceLinkedRole:
          props.createEmrOnEksServiceLinkedRole ?? false,
      });

      new cdk.CfnOutput(this, "emrOnEksJobExecutionRoleArn", {
        value: this.emrVirtualCluster.jobRole.roleArn,
        description: "jOB_EXECUTION_ROLE_ARN",
      });
      new cdk.CfnOutput(this, "emrOnEksVirtualClusterId", {
        value: this.emrVirtualCluster.virtualClusterId,
        description: "VIRTUAL_CLUSTER_ID",
      });

      // With EMR on EKS, each job can have a different runtime role
      // We create that role in Application construct, but grant it the
      // necessary access to write logs and read/write data here.
      sampleData.grantRead(this.emrVirtualCluster);
      sampleData.grantWrite(this.emrVirtualCluster);
      props.bucket.grantRead(this.emrVirtualCluster);
      props.bucket.grantWrite(this.emrVirtualCluster, "logs/*");
      props.bucket.grantWrite(this.emrVirtualCluster, "output/*");
      this.emrVirtualCluster.grantManage(dataEngineerRole);
    }

    if (props.createEMRStudio) {
      this.emrStudio = new Studio(this, "EMRStudio", {
        vpc: props.vpc,
        bucket: props.bucket,
      });
      new cdk.CfnOutput(this, "emrStudioUrl", {
        value: this.emrStudio.studioUrl,
        description: "STUDIO_URL",
      });

      adminPrincipals?.forEach((principal) => {
        // Commenting this out b/c for users it easily exceeds the max policy size
        // this.emrStudio.grantConsoleAccess(principal);
        if (principal instanceof iam.User) {
          // This seems to cause an issue when trying to delete the stack :\
          // Failed to destroy AnalyticsPOCStack: UPDATE_COMPLETE (Export AnalyticsPOCStack:ExportsOutputRefEMRStudioStudioUserPolicyE3A1032A4576EE56 cannot be deleted as it is in use by POCPermissionsStack)
          this.emrStudio.addUser(principal, this.emr);
          this.emrStudio.addUser(principal, this.emrApplication);
        }
      });
    }

    // Also, what I'd like to do is:
    // s3Bucket.grantRead(emr)
    // emrs.grantExecute(dataEngineer)
    // emr.grantStudioAccess(emrStudio)
    if (props.createAthenaSQL) {
      this.athenaSQL = new AthenaSQL(this, "AthenaSQL", {
        bucket: props.bucket,
      });
      this.athenaSQL.grantExecute(dataEngineerRole);
    }

    if (props.createAthenaSpark) {
      this.athenaSpark = new AthenaSpark(this, "AthenaSpark", {
        bucket: props.bucket,
      });
      this.athenaSpark.grantExecute(dataEngineerRole);
    }

    // Create a Lake Formation database / bucket with limited access
    if (props.createLakeFormationDatabase) {
      const lf = new LakeFormation(this, "LakeFormation");
      lf.grantGlue(dataEngineerRole);
      (
        adminPrincipals?.filter((x) => x instanceof iam.User) as iam.User[]
      ).forEach((x) => {
        lf.grantWrite((x as iam.User).userArn);
      });
    }

    // Finally, with everything done, we deploy a nice little landing page
    // Temporarily disabled until a better way to create this is figured out.
    // const landingpage = new LandingPage(this, "LandingPage", {
    //   bucket: props.bucket,
    //   emrStudioURL: this.emrStudio?.studioUrl,
    //   emrServerlessApplicationID: this.emrApplication?.applicationId,
    // });
    // new cdk.CfnOutput(this, "LandingPageURL", {
    //   value: landingpage.url,
    //   description: "Analytics PoC Landing Page",
    // });

    // In which we create sample code for a streaming app
    if (props.dataSources) {
      props.dataSources?.forEach((k) => {
        const stream = cdk.aws_kinesis.Stream.fromStreamArn(this, "KStream", k);
        stream.grantRead(this.emrApplication);

        // For now, we only support one stream name
        new cdk.CfnOutput(this, "streamName", { value: stream.streamName });
        new cdk.CfnOutput(this, "region", { value: this.region });
      });
      new SampleStreamingApp(this, "StreamingApp", {
        artifactsBucket: props.bucket,
      });
    }
  }

  genericUser(id: string, username: string): iam.IUser {
    return new iam.User(this, id, {
      userName: username,
      password: cdk.SecretValue.unsafePlainText("This1IsATerriblePassword!"),
    });
  }
}
