import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";
import * as random from "@pulumi/pulumi";

export interface SecretsResult {
  authSecret: aws.secretsmanager.Secret;
  authSecretValue: aws.secretsmanager.SecretVersion;
  dbPassword: aws.secretsmanager.Secret;
  dbPasswordValue: aws.secretsmanager.SecretVersion;
  dbPasswordPlain: pulumi.Output<string>;
  authSecretPlain: pulumi.Output<string>;
}

export function createSecrets(): SecretsResult {
  // Generate random passwords
  const dbPasswordPlain = new pulumi.Config("deploy").getSecret("dbPassword") ??
    pulumi.output("change-me-in-production-32chars!!");
  const authSecretPlain = new pulumi.Config("deploy").getSecret("authSecret") ??
    pulumi.output("change-me-auth-secret-32chars!!!");

  const dbPassword = new aws.secretsmanager.Secret("trex-db-password", {
    name: "trex/db-password",
  });

  const dbPasswordValue = new aws.secretsmanager.SecretVersion("trex-db-password-val", {
    secretId: dbPassword.id,
    secretString: dbPasswordPlain,
  });

  const authSecret = new aws.secretsmanager.Secret("trex-auth-secret", {
    name: "trex/auth-secret",
  });

  const authSecretValue = new aws.secretsmanager.SecretVersion("trex-auth-secret-val", {
    secretId: authSecret.id,
    secretString: authSecretPlain,
  });

  return {
    authSecret,
    authSecretValue,
    dbPassword,
    dbPasswordValue,
    dbPasswordPlain,
    authSecretPlain,
  };
}
