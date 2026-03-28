import * as azure from "@pulumi/azure-native";
import * as pulumi from "@pulumi/pulumi";

export interface StorageResult {
  account: azure.storage.StorageAccount;
  container: azure.storage.BlobContainer;
  s3Endpoint: pulumi.Output<string>;
  accessKey: pulumi.Output<string>;
}

export function createStorage(opts: {
  env: string;
  resourceGroupName: pulumi.Input<string>;
  location: pulumi.Input<string>;
}): StorageResult {
  const account = new azure.storage.StorageAccount(`trex${opts.env}storage`, {
    resourceGroupName: opts.resourceGroupName,
    location: opts.location,
    sku: { name: "Standard_LRS" },
    kind: "StorageV2",
    allowBlobPublicAccess: false,
    isHnsEnabled: true, // Data Lake Storage Gen2 for S3 compatibility
    minimumTlsVersion: "TLS1_2",
    encryption: {
      services: {
        blob: { enabled: true, keyType: "Account" },
      },
      keySource: "Microsoft.Storage",
    },
  });

  const container = new azure.storage.BlobContainer(`trex-${opts.env}-storage-container`, {
    resourceGroupName: opts.resourceGroupName,
    accountName: account.name,
    containerName: `trex-${opts.env}-storage`,
    publicAccess: "None",
  });

  // Get storage account keys for S3-compatible access
  const keys = azure.storage.listStorageAccountKeysOutput({
    resourceGroupName: opts.resourceGroupName,
    accountName: account.name,
  });

  const accessKey = keys.apply((k) => k.keys[0].value);

  const s3Endpoint = pulumi.interpolate`https://${account.name}.blob.core.windows.net`;

  return { account, container, s3Endpoint, accessKey };
}
