import { Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';


interface S3GrantFractalbitsBuildsBucketStackProps extends StackProps {
  targetAccountId: string;
}

export class S3GrantFractalbitsBuildsBucketStack extends Stack {
  constructor(scope: Construct, id: string, props: S3GrantFractalbitsBuildsBucketStackProps) {
    super(scope, id, props);

    const region = Stack.of(this).region;
    const bucket = s3.Bucket.fromBucketName(this, 'BuildsBucket', `fractalbits-builds-${region}`);


    const targetUserArn = `arn:aws:iam::${props.targetAccountId}:root`;

    // Grant only s3:GetObject permission
    bucket.addToResourcePolicy(new iam.PolicyStatement({
      actions: ['s3:GetObject'],
      effect: iam.Effect.ALLOW,
      principals: [new iam.ArnPrincipal(targetUserArn)],
      resources: [`${bucket.bucketArn}/*`],
    }));
  }
}
