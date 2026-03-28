import * as aws from "@pulumi/aws";

export interface S3Result {
  bucket: aws.s3.BucketV2;
}

export function createS3(): S3Result {
  const bucket = new aws.s3.BucketV2("trex-storage", {
    forceDestroy: false,
  });

  new aws.s3.BucketPublicAccessBlock("trex-storage-block", {
    bucket: bucket.id,
    blockPublicAcls: true,
    blockPublicPolicy: true,
    ignorePublicAcls: true,
    restrictPublicBuckets: true,
  });

  new aws.s3.BucketServerSideEncryptionConfigurationV2("trex-storage-enc", {
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
