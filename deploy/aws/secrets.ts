import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";

export interface SecretsResult {
  authSecret: aws.secretsmanager.Secret;
  authSecretValue: aws.secretsmanager.SecretVersion;
  dbPassword: aws.secretsmanager.Secret;
  dbPasswordValue: aws.secretsmanager.SecretVersion;
  dbPasswordPlain: pulumi.Output<string>;
  authSecretPlain: pulumi.Output<string>;
}

export function createSecrets(env: string): SecretsResult {
  const cfg = new pulumi.Config("deploy");

  // Require secrets to be set explicitly via:
  //   pulumi config set --secret deploy:dbPassword <value>
  //   pulumi config set --secret deploy:authSecret <value>
  // requireSecret() throws a clear error if the value is missing, naming the
  // exact config key so the operator knows what to set.
  const dbPasswordPlain = cfg.requireSecret("dbPassword");
  const authSecretPlain = cfg.requireSecret("authSecret");

  // Recovery window for AWS Secrets Manager soft-delete. Default 7 days
  // (AWS-recommended minimum for production). Operators can opt into immediate
  // deletion by explicitly setting `deploy:secretRecoveryWindowDays` to 0 — only
  // appropriate for short-lived dev stacks.
  const recoveryWindowInDays = cfg.getNumber("secretRecoveryWindowDays") ?? 7;

  const dbPassword = new aws.secretsmanager.Secret(`trex-${env}-db-password`, {
    name: `trex-${env}/db-password`,
    recoveryWindowInDays,
  });

  const dbPasswordValue = new aws.secretsmanager.SecretVersion(`trex-${env}-db-password-val`, {
    secretId: dbPassword.id,
    secretString: dbPasswordPlain,
  });

  const authSecret = new aws.secretsmanager.Secret(`trex-${env}-auth-secret`, {
    name: `trex-${env}/auth-secret`,
    recoveryWindowInDays,
  });

  const authSecretValue = new aws.secretsmanager.SecretVersion(`trex-${env}-auth-secret-val`, {
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
