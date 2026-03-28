import * as pulumi from "@pulumi/pulumi";
import { DeployConfig, getSizing } from "../shared/config";
import { exportOutputs, StackOutputs } from "../shared/outputs";
import { createNetworking } from "./networking";
import { createRds } from "./rds";
import { createS3 } from "./s3";
import { createSecrets } from "./secrets";
import { createEcs } from "./ecs";

export function deployAws(config: DeployConfig) {
  const sizing = getSizing("aws", config.environment);

  // Secrets (DB password, auth secret)
  const secrets = createSecrets();

  // ACM certificate — user must provide ARN via config or create one
  const certArn = new pulumi.Config("deploy").require("certificateArn");

  // Networking (VPC, ALB, security groups, EFS)
  const networking = createNetworking(sizing, certArn);

  // RDS PostgreSQL
  const rds = createRds(
    sizing,
    networking.vpc.vpcId,
    networking.vpc.privateSubnetIds,
    networking.rdsSecurityGroup.id,
    secrets.dbPasswordPlain
  );

  // S3 bucket for storage plugin
  const s3 = createS3();

  // ECS Fargate
  const endpointUrl = pulumi.interpolate`https://${networking.alb.dnsName}`;

  const ecs = createEcs({
    sizing,
    ghcrImage: config.ghcrImage,
    vpcId: networking.vpc.vpcId,
    subnetIds: networking.vpc.privateSubnetIds,
    securityGroupId: networking.ecsSecurityGroup.id,
    targetGroupArn: networking.targetGroup.arn,
    efsId: networking.efs.id,
    efsAccessPointId: networking.efsAccessPoint.id,
    databaseUrl: rds.connectionString,
    authSecret: secrets.authSecretPlain,
    endpointUrl,
    s3BucketName: s3.bucket.bucket,
  });

  const outputs: StackOutputs = {
    endpointUrl,
    dbHost: rds.instance.endpoint,
    storageEndpoint: pulumi.interpolate`s3://${s3.bucket.bucket}`,
  };

  return exportOutputs(outputs);
}
