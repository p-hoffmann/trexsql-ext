import * as azure from "@pulumi/azure-native";
import * as pulumi from "@pulumi/pulumi";

export interface NetworkingResult {
  resourceGroup: azure.resources.ResourceGroup;
  vnet: azure.network.VirtualNetwork;
  containerAppsSubnet: azure.network.Subnet;
  postgresSubnet: azure.network.Subnet;
}

export function createNetworking(
  region: string
): NetworkingResult {
  const resourceGroup = new azure.resources.ResourceGroup("trex-rg", {
    location: region,
  });

  const vnet = new azure.network.VirtualNetwork("trex-vnet", {
    resourceGroupName: resourceGroup.name,
    location: resourceGroup.location,
    addressSpace: { addressPrefixes: ["10.0.0.0/16"] },
  });

  const containerAppsSubnet = new azure.network.Subnet("trex-aca-subnet", {
    resourceGroupName: resourceGroup.name,
    virtualNetworkName: vnet.name,
    addressPrefix: "10.0.0.0/23",
    delegations: [
      {
        name: "Microsoft.App.environments",
        serviceName: "Microsoft.App/environments",
      },
    ],
  });

  const postgresSubnet = new azure.network.Subnet("trex-pg-subnet", {
    resourceGroupName: resourceGroup.name,
    virtualNetworkName: vnet.name,
    addressPrefix: "10.0.2.0/24",
    delegations: [
      {
        name: "Microsoft.DBforPostgreSQL.flexibleServers",
        serviceName: "Microsoft.DBforPostgreSQL/flexibleServers",
      },
    ],
  });

  return { resourceGroup, vnet, containerAppsSubnet, postgresSubnet };
}
