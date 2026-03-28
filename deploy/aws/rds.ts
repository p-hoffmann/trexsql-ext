import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";
import { Sizing } from "../shared/config";

export interface RdsResult {
  instance: aws.rds.Instance;
  connectionString: pulumi.Output<string>;
}

export function createRds(
  sizing: Sizing,
  vpcId: pulumi.Input<string>,
  subnetIds: pulumi.Input<string[]>,
  securityGroupId: pulumi.Input<string>,
  dbPassword: pulumi.Input<string>
): RdsResult {
  const subnetGroup = new aws.rds.SubnetGroup("trex-db-subnet", {
    subnetIds,
  });

  const instance = new aws.rds.Instance("trex-db", {
    engine: "postgres",
    engineVersion: "16",
    instanceClass: sizing.dbInstanceClass,
    allocatedStorage: sizing.dbStorageGb,
    storageType: "gp3",
    dbName: "trex",
    username: "postgres",
    password: dbPassword,
    dbSubnetGroupName: subnetGroup.name,
    vpcSecurityGroupIds: [securityGroupId],
    multiAz: sizing.dbMultiAz,
    backupRetentionPeriod: sizing.dbMultiAz ? 7 : 1,
    skipFinalSnapshot: !sizing.dbMultiAz,
    finalSnapshotIdentifier: sizing.dbMultiAz ? "trex-db-final" : undefined,
    publiclyAccessible: false,
  });

  const connectionString = pulumi.interpolate`postgres://${instance.username}:${dbPassword}@${instance.endpoint}/${instance.dbName}`;

  return { instance, connectionString };
}
