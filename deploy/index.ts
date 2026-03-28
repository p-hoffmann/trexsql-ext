import { getConfig } from "./shared/config";
import { deployAws } from "./aws";
import { deployAzure } from "./azure";

const config = getConfig();

const outputs =
  config.cloud === "aws" ? deployAws(config) : deployAzure(config);

export const endpointUrl = outputs.endpointUrl;
export const dbHost = outputs.dbHost;
export const storageEndpoint = outputs.storageEndpoint;
