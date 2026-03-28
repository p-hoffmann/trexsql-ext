import * as pulumi from "@pulumi/pulumi";

export type Cloud = "aws" | "azure";
export type Environment = "dev" | "prod";

export interface DeployConfig {
  cloud: Cloud;
  environment: Environment;
  ghcrImage: string;
  region: string;
}

export interface Sizing {
  cpu: number;
  memory: number;
  minReplicas: number;
  maxReplicas: number;
  dbInstanceClass: string;
  dbStorageGb: number;
  dbMultiAz: boolean;
}

const sizingMap: Record<Environment, Record<Cloud, Sizing>> = {
  dev: {
    aws: {
      cpu: 2048, // 2 vCPU (Fargate uses MiB units)
      memory: 4096, // 4 GB
      minReplicas: 1,
      maxReplicas: 1,
      dbInstanceClass: "db.t4g.micro",
      dbStorageGb: 20,
      dbMultiAz: false,
    },
    azure: {
      cpu: 1,
      memory: 2,
      minReplicas: 1,
      maxReplicas: 2,
      dbInstanceClass: "B_Standard_B1ms",
      dbStorageGb: 32,
      dbMultiAz: false,
    },
  },
  prod: {
    aws: {
      cpu: 4096, // 4 vCPU
      memory: 8192, // 8 GB
      minReplicas: 2,
      maxReplicas: 4,
      dbInstanceClass: "db.r6g.large",
      dbStorageGb: 100,
      dbMultiAz: true,
    },
    azure: {
      cpu: 2,
      memory: 4,
      minReplicas: 2,
      maxReplicas: 4,
      dbInstanceClass: "GP_Standard_D2s_v3",
      dbStorageGb: 128,
      dbMultiAz: true,
    },
  },
};

export function getConfig(): DeployConfig {
  const config = new pulumi.Config("deploy");
  return {
    cloud: config.require("cloud") as Cloud,
    environment: config.require("environment") as Environment,
    ghcrImage: config.require("ghcrImage"),
    region: config.require("region"),
  };
}

export function getSizing(cloud: Cloud, environment: Environment): Sizing {
  return sizingMap[environment][cloud];
}
