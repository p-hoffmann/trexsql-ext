import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";
import { Sizing } from "../shared/config";
import {
  TREX_PORT,
  POSTGREST_PORT,
  POSTGREST_IMAGE,
  TREX_HEALTH_CHECK,
  buildTrexEnvVars,
  buildPostgrestEnvVars,
} from "../shared/containers";

export interface EcsResult {
  cluster: aws.ecs.Cluster;
  service: aws.ecs.Service;
  taskDefinition: aws.ecs.TaskDefinition;
}

export function createEcs(opts: {
  sizing: Sizing;
  ghcrImage: string;
  vpcId: pulumi.Input<string>;
  subnetIds: pulumi.Input<string[]>;
  securityGroupId: pulumi.Input<string>;
  targetGroupArn: pulumi.Input<string>;
  efsId: pulumi.Input<string>;
  efsAccessPointId: pulumi.Input<string>;
  databaseUrl: pulumi.Input<string>;
  authSecret: pulumi.Input<string>;
  endpointUrl: pulumi.Input<string>;
  s3BucketName: pulumi.Input<string>;
}): EcsResult {
  const cluster = new aws.ecs.Cluster("trex-cluster", {
    settings: [{ name: "containerInsights", value: "enabled" }],
  });

  // IAM roles
  const executionRole = new aws.iam.Role("trex-execution-role", {
    assumeRolePolicy: JSON.stringify({
      Version: "2012-10-17",
      Statement: [
        {
          Action: "sts:AssumeRole",
          Effect: "Allow",
          Principal: { Service: "ecs-tasks.amazonaws.com" },
        },
      ],
    }),
  });

  new aws.iam.RolePolicyAttachment("trex-execution-policy", {
    role: executionRole.name,
    policyArn: "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
  });

  const taskRole = new aws.iam.Role("trex-task-role", {
    assumeRolePolicy: JSON.stringify({
      Version: "2012-10-17",
      Statement: [
        {
          Action: "sts:AssumeRole",
          Effect: "Allow",
          Principal: { Service: "ecs-tasks.amazonaws.com" },
        },
      ],
    }),
  });

  // S3 access for storage plugin
  new aws.iam.RolePolicy("trex-task-s3-policy", {
    role: taskRole.name,
    policy: pulumi.output(opts.s3BucketName).apply((bucket) =>
      JSON.stringify({
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Action: ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
            Resource: [`arn:aws:s3:::${bucket}`, `arn:aws:s3:::${bucket}/*`],
          },
        ],
      })
    ),
  });

  // CloudWatch log group
  const logGroup = new aws.cloudwatch.LogGroup("trex-logs", {
    retentionInDays: 30,
  });

  // Build environment variables
  const trexEnv = pulumi.all([opts.databaseUrl, opts.authSecret, opts.endpointUrl]).apply(
    ([dbUrl, secret, endpoint]) =>
      buildTrexEnvVars({ databaseUrl: dbUrl, authSecret: secret, endpointUrl: endpoint })
  );

  const postgrestEnv = pulumi.all([opts.databaseUrl, opts.authSecret, opts.endpointUrl]).apply(
    ([dbUrl, secret, endpoint]) =>
      buildPostgrestEnvVars({ databaseUrl: dbUrl, jwtSecret: secret, endpointUrl: endpoint })
  );

  // Task definition
  const containerDefinitions = pulumi
    .all([trexEnv, postgrestEnv, opts.s3BucketName])
    .apply(([tEnv, pEnv, bucketName]) =>
      JSON.stringify([
        {
          name: "trex",
          image: opts.ghcrImage,
          essential: true,
          portMappings: [{ containerPort: TREX_PORT, protocol: "tcp" }],
          environment: [
            ...Object.entries(tEnv).map(([name, value]) => ({ name, value })),
            { name: "STORAGE_S3_BUCKET", value: bucketName },
          ],
          mountPoints: [
            {
              sourceVolume: "devx-workspaces",
              containerPath: "/tmp/devx-workspaces",
            },
          ],
          healthCheck: {
            command: ["CMD-SHELL", `curl -f http://localhost:${TREX_PORT}${TREX_HEALTH_CHECK.path} || exit 1`],
            interval: TREX_HEALTH_CHECK.intervalSeconds,
            timeout: TREX_HEALTH_CHECK.timeoutSeconds,
            retries: TREX_HEALTH_CHECK.unhealthyThreshold,
          },
          logConfiguration: {
            logDriver: "awslogs",
            options: {
              "awslogs-group": logGroup.name,
              "awslogs-region": aws.getRegionOutput().name,
              "awslogs-stream-prefix": "trex",
            },
          },
        },
        {
          name: "postgrest",
          image: POSTGREST_IMAGE,
          essential: true,
          portMappings: [{ containerPort: POSTGREST_PORT, protocol: "tcp" }],
          environment: Object.entries(pEnv).map(([name, value]) => ({
            name,
            value,
          })),
          logConfiguration: {
            logDriver: "awslogs",
            options: {
              "awslogs-group": logGroup.name,
              "awslogs-region": aws.getRegionOutput().name,
              "awslogs-stream-prefix": "postgrest",
            },
          },
        },
      ])
    );

  const taskDefinition = new aws.ecs.TaskDefinition("trex-task", {
    family: "trex",
    networkMode: "awsvpc",
    requiresCompatibilities: ["FARGATE"],
    cpu: String(opts.sizing.cpu),
    memory: String(opts.sizing.memory),
    executionRoleArn: executionRole.arn,
    taskRoleArn: taskRole.arn,
    containerDefinitions,
    volumes: [
      {
        name: "devx-workspaces",
        efsVolumeConfiguration: {
          fileSystemId: opts.efsId,
          transitEncryption: "ENABLED",
          authorizationConfig: {
            accessPointId: opts.efsAccessPointId,
            iam: "ENABLED",
          },
        },
      },
    ],
  });

  const service = new aws.ecs.Service("trex-service", {
    cluster: cluster.arn,
    taskDefinition: taskDefinition.arn,
    desiredCount: opts.sizing.minReplicas,
    launchType: "FARGATE",
    networkConfiguration: {
      subnets: opts.subnetIds,
      securityGroups: [opts.securityGroupId],
      assignPublicIp: false,
    },
    loadBalancers: [
      {
        targetGroupArn: opts.targetGroupArn,
        containerName: "trex",
        containerPort: TREX_PORT,
      },
    ],
  });

  return { cluster, service, taskDefinition };
}
