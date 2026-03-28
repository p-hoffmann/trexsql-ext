import * as pulumi from "@pulumi/pulumi";
import { DeployConfig, getSizing } from "../shared/config";
import { exportOutputs, StackOutputs } from "../shared/outputs";
import { createNetworking } from "./networking";
import { createPostgres } from "./postgres";
import { createStorage } from "./storage";
import { createSecrets } from "./secrets";
import { createContainerApps } from "./container-apps";

export function deployAzure(config: DeployConfig) {
  const sizing = getSizing("azure", config.environment);

  // Secrets
  const secrets = createSecrets();

  // Networking (resource group, VNet, subnets)
  const networking = createNetworking(config.region);

  // PostgreSQL Flexible Server
  const postgres = createPostgres({
    sizing,
    resourceGroupName: networking.resourceGroup.name,
    location: networking.resourceGroup.location,
    subnetId: networking.postgresSubnet.id,
    vnetId: networking.vnet.id,
    adminPassword: secrets.dbPasswordPlain,
  });

  // Blob Storage with S3-compatible access
  const storage = createStorage({
    resourceGroupName: networking.resourceGroup.name,
    location: networking.resourceGroup.location,
  });

  // Container Apps
  const containerApps = createContainerApps({
    sizing,
    ghcrImage: config.ghcrImage,
    resourceGroupName: networking.resourceGroup.name,
    location: networking.resourceGroup.location,
    subnetId: networking.containerAppsSubnet.id,
    databaseUrl: postgres.connectionString,
    authSecret: secrets.authSecretPlain,
    s3Endpoint: storage.s3Endpoint,
    s3AccessKey: storage.accessKey,
    s3BucketName: "trex-storage",
  });

  const endpointUrl = containerApps.app.latestRevisionFqdn.apply(
    (fqdn) => `https://${fqdn}`
  );

  const outputs: StackOutputs = {
    endpointUrl,
    dbHost: postgres.server.fullyQualifiedDomainName.apply((h) => h ?? ""),
    storageEndpoint: storage.s3Endpoint,
  };

  return exportOutputs(outputs);
}
