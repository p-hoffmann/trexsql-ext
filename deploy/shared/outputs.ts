import * as pulumi from "@pulumi/pulumi";

export interface StackOutputs {
  endpointUrl: pulumi.Output<string>;
  dbHost: pulumi.Output<string>;
  storageEndpoint: pulumi.Output<string>;
}

export function exportOutputs(outputs: StackOutputs) {
  return {
    endpointUrl: outputs.endpointUrl,
    dbHost: pulumi.secret(outputs.dbHost),
    storageEndpoint: outputs.storageEndpoint,
  };
}
