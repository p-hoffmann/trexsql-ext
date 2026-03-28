import * as azure from "@pulumi/azure-native";
import * as pulumi from "@pulumi/pulumi";
import { Sizing } from "../shared/config";

export interface PostgresResult {
  server: azure.dbforpostgresql.Server;
  connectionString: pulumi.Output<string>;
}

export function createPostgres(opts: {
  env: string;
  sizing: Sizing;
  resourceGroupName: pulumi.Input<string>;
  location: pulumi.Input<string>;
  subnetId: pulumi.Input<string>;
  vnetId: pulumi.Input<string>;
  adminPassword: pulumi.Input<string>;
}): PostgresResult {
  const privateDnsZone = new azure.network.PrivateZone(`trex-${opts.env}-pg-dns`, {
    resourceGroupName: opts.resourceGroupName,
    location: "Global",
    privateZoneName: `trex-${opts.env}.postgres.database.azure.com`,
  });

  new azure.network.VirtualNetworkLink(`trex-${opts.env}-pg-dns-link`, {
    resourceGroupName: opts.resourceGroupName,
    privateZoneName: privateDnsZone.name,
    location: "Global",
    virtualNetwork: { id: opts.vnetId },
    registrationEnabled: false,
  });

  const server = new azure.dbforpostgresql.Server(`trex-${opts.env}-pg`, {
    resourceGroupName: opts.resourceGroupName,
    location: opts.location,
    version: "16",
    sku: {
      name: opts.sizing.dbInstanceClass,
      tier: opts.sizing.dbMultiAz ? "GeneralPurpose" : "Burstable",
    },
    storage: {
      storageSizeGB: opts.sizing.dbStorageGb,
    },
    administratorLogin: "postgres",
    administratorLoginPassword: opts.adminPassword,
    network: {
      delegatedSubnetResourceId: opts.subnetId,
      privateDnsZoneArmResourceId: privateDnsZone.id,
    },
    highAvailability: opts.sizing.dbMultiAz
      ? { mode: "ZoneRedundant" }
      : { mode: "Disabled" },
    backup: {
      backupRetentionDays: opts.sizing.dbMultiAz ? 7 : 1,
      geoRedundantBackup: opts.sizing.dbMultiAz ? "Enabled" : "Disabled",
    },
  });

  // Create the trex database
  new azure.dbforpostgresql.Database(`trex-${opts.env}-db`, {
    resourceGroupName: opts.resourceGroupName,
    serverName: server.name,
    databaseName: "trex",
    charset: "UTF8",
  });

  const connectionString = pulumi.interpolate`postgres://postgres:${opts.adminPassword}@${server.fullyQualifiedDomainName}/trex?sslmode=require`;

  return { server, connectionString };
}
