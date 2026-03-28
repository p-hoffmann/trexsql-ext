import * as aws from "@pulumi/aws";

export interface S3Result {
  bucket: aws.s3.BucketV2;
}

export function createS3(env: string): S3Result {
  const bucket = new aws.s3.BucketV2(`trex-${env}-storage`, {
    forceDestroy: false,
  });

  new aws.s3.BucketPublicAccessBlock(`trex-${env}-storage-block`, {
    bucket: bucket.id,
    blockPublicAcls: true,
    blockPublicPolicy: true,
    ignorePublicAcls: true,
    restrictPublicBuckets: true,
  });

  new aws.s3.BucketServerSideEncryptionConfigurationV2(`trex-${env}-storage-enc`, {
    bucket: bucket.id,
    rules: [
      {
        applyServerSideEncryptionByDefault: {
          sseAlgorithm: "AES256",
        },
      },
    ],
  });

  return { bucket };
}
