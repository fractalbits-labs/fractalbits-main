import * as cdk from 'aws-cdk-lib';
import {Construct} from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as cr from 'aws-cdk-lib/custom-resources';

export class FractalbitsHelperStack extends cdk.Stack {
  public readonly deregisterProviderServiceToken: string;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const deregisterLambdaRole = new iam.Role(this, 'DeregisterLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AWSCloudMapFullAccess'),
      ],
    });

    const deregisterLambda = new lambda.Function(this, 'DeregisterLambda', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('lib/lambda/deregister-asg-instances'),
      role: deregisterLambdaRole,
      timeout: cdk.Duration.seconds(60),
    });

    const deregisterProvider = new cr.Provider(this, 'DeregisterProvider', {
      onEventHandler: deregisterLambda,
    });

    this.deregisterProviderServiceToken = deregisterProvider.serviceToken;
  }
}
