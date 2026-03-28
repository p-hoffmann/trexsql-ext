import * as aws from "@pulumi/aws";
import * as awsx from "@pulumi/awsx";
import * as pulumi from "@pulumi/pulumi";
import { Sizing } from "../shared/config";
import { TREX_PORT, TREX_HEALTH_CHECK } from "../shared/containers";

export interface NetworkingResult {
  vpc: awsx.ec2.Vpc;
  albSecurityGroup: aws.ec2.SecurityGroup;
  ecsSecurityGroup: aws.ec2.SecurityGroup;
  rdsSecurityGroup: aws.ec2.SecurityGroup;
  alb: aws.lb.LoadBalancer;
  targetGroup: aws.lb.TargetGroup;
  httpsListener: aws.lb.Listener;
  efs: aws.efs.FileSystem;
  efsSecurityGroup: aws.ec2.SecurityGroup;
  efsMountTargets: aws.efs.MountTarget[];
  efsAccessPoint: aws.efs.AccessPoint;
}

export function createNetworking(
  sizing: Sizing,
  certificateArn: pulumi.Input<string>
): NetworkingResult {
  const vpc = new awsx.ec2.Vpc("trex-vpc", {
    numberOfAvailabilityZones: 2,
    natGateways: { strategy: awsx.ec2.NatGatewayStrategy.Single },
  });

  // Security Groups
  const albSecurityGroup = new aws.ec2.SecurityGroup("trex-alb-sg", {
    vpcId: vpc.vpcId,
    ingress: [
      { protocol: "tcp", fromPort: 443, toPort: 443, cidrBlocks: ["0.0.0.0/0"] },
      { protocol: "tcp", fromPort: 80, toPort: 80, cidrBlocks: ["0.0.0.0/0"] },
    ],
    egress: [
      { protocol: "-1", fromPort: 0, toPort: 0, cidrBlocks: ["0.0.0.0/0"] },
    ],
  });

  const ecsSecurityGroup = new aws.ec2.SecurityGroup("trex-ecs-sg", {
    vpcId: vpc.vpcId,
    ingress: [
      {
        protocol: "tcp",
        fromPort: TREX_PORT,
        toPort: TREX_PORT,
        securityGroups: [albSecurityGroup.id],
      },
    ],
    egress: [
      { protocol: "-1", fromPort: 0, toPort: 0, cidrBlocks: ["0.0.0.0/0"] },
    ],
  });

  const rdsSecurityGroup = new aws.ec2.SecurityGroup("trex-rds-sg", {
    vpcId: vpc.vpcId,
    ingress: [
      {
        protocol: "tcp",
        fromPort: 5432,
        toPort: 5432,
        securityGroups: [ecsSecurityGroup.id],
      },
    ],
    egress: [
      { protocol: "-1", fromPort: 0, toPort: 0, cidrBlocks: ["0.0.0.0/0"] },
    ],
  });

  // EFS for DuckDB workspace persistence
  const efsSecurityGroup = new aws.ec2.SecurityGroup("trex-efs-sg", {
    vpcId: vpc.vpcId,
    ingress: [
      {
        protocol: "tcp",
        fromPort: 2049,
        toPort: 2049,
        securityGroups: [ecsSecurityGroup.id],
      },
    ],
    egress: [
      { protocol: "-1", fromPort: 0, toPort: 0, cidrBlocks: ["0.0.0.0/0"] },
    ],
  });

  const efs = new aws.efs.FileSystem("trex-efs", {
    encrypted: true,
    tags: { Name: "trex-workspaces" },
  });

  const efsMountTargets = vpc.privateSubnetIds.apply((subnetIds) =>
    subnetIds.map(
      (subnetId, i) =>
        new aws.efs.MountTarget(`trex-efs-mt-${i}`, {
          fileSystemId: efs.id,
          subnetId,
          securityGroups: [efsSecurityGroup.id],
        })
    )
  );

  const efsAccessPoint = new aws.efs.AccessPoint("trex-efs-ap", {
    fileSystemId: efs.id,
    rootDirectory: {
      path: "/devx-workspaces",
      creationInfo: { ownerGid: 1000, ownerUid: 1000, permissions: "755" },
    },
    posixUser: { gid: 1000, uid: 1000 },
  });

  // Application Load Balancer
  const alb = new aws.lb.LoadBalancer("trex-alb", {
    securityGroups: [albSecurityGroup.id],
    subnets: vpc.publicSubnetIds,
    loadBalancerType: "application",
  });

  const targetGroup = new aws.lb.TargetGroup("trex-tg", {
    port: TREX_PORT,
    protocol: "HTTP",
    vpcId: vpc.vpcId,
    targetType: "ip",
    healthCheck: {
      path: TREX_HEALTH_CHECK.path,
      port: String(TREX_HEALTH_CHECK.port),
      interval: TREX_HEALTH_CHECK.intervalSeconds,
      timeout: TREX_HEALTH_CHECK.timeoutSeconds,
      healthyThreshold: TREX_HEALTH_CHECK.healthyThreshold,
      unhealthyThreshold: TREX_HEALTH_CHECK.unhealthyThreshold,
    },
  });

  // HTTP → HTTPS redirect
  new aws.lb.Listener("trex-http-redirect", {
    loadBalancerArn: alb.arn,
    port: 80,
    protocol: "HTTP",
    defaultActions: [
      {
        type: "redirect",
        redirect: { port: "443", protocol: "HTTPS", statusCode: "HTTP_301" },
      },
    ],
  });

  const httpsListener = new aws.lb.Listener("trex-https", {
    loadBalancerArn: alb.arn,
    port: 443,
    protocol: "HTTPS",
    certificateArn,
    defaultActions: [{ type: "forward", targetGroupArn: targetGroup.arn }],
  });

  return {
    vpc,
    albSecurityGroup,
    ecsSecurityGroup,
    rdsSecurityGroup,
    alb,
    targetGroup,
    httpsListener,
    efs,
    efsSecurityGroup,
    efsMountTargets: efsMountTargets as any,
    efsAccessPoint,
  };
}
