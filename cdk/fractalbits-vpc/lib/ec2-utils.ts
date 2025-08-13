import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as autoscaling from 'aws-cdk-lib/aws-autoscaling';
import {Construct} from 'constructs';
import * as servicediscovery from 'aws-cdk-lib/aws-servicediscovery';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as hooktargets from 'aws-cdk-lib/aws-autoscaling-hooktargets';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as path from 'path';

export const createVpcEndpoints = (vpc: ec2.Vpc) => {
  // Add Gateway Endpoint for S3
  vpc.addGatewayEndpoint('S3Endpoint', {
    service: ec2.GatewayVpcEndpointAwsService.S3,
  });

  // Add Gateway Endpoint for S3 Express One Zone
  // S3 Express requires a separate gateway endpoint for optimal performance
  vpc.addGatewayEndpoint('S3ExpressEndpoint', {
    service: new ec2.GatewayVpcEndpointAwsService('s3express'),
  });

  // Add Gateway Endpoint for DynamoDB
  vpc.addGatewayEndpoint('DynamoDbEndpoint', {
    service: ec2.GatewayVpcEndpointAwsService.DYNAMODB,
  });

  // Add Interface Endpoint for EC2, SSM, and CloudWatch
  ['SSM', 'SSM_MESSAGES', 'EC2', 'EC2_MESSAGES', 'CLOUDWATCH', 'CLOUDWATCH_LOGS'].forEach(service => {
    vpc.addInterfaceEndpoint(`${service}Endpoint`, {
      service: (ec2.InterfaceVpcEndpointAwsService as any)[service],
      subnets: {subnetType: ec2.SubnetType.PRIVATE_ISOLATED},
      privateDnsEnabled: true,
    });
  });
};

export const createEc2Role = (scope: Construct): iam.Role => {
  const role = new iam.Role(scope, 'InstanceRole', {
    assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
    managedPolicies: [
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMFullAccess'),
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonDynamoDBFullAccess_v2'),
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonEC2FullAccess'),
      iam.ManagedPolicy.fromAwsManagedPolicyName('CloudWatchAgentServerPolicy'),
    ],
  });

  // Add S3 Express One Zone permissions
  role.addToPolicy(new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: [
      's3express:CreateSession',
      's3express:DeleteSession',
      's3express:PutObject',
      's3express:GetObject',
      's3express:DeleteObject',
      's3express:ListBucket'
    ],
    resources: ['*']
  }));

  return role;
};

export const createServiceDiscoveryTable = (scope: Construct): dynamodb.Table => {
  return new dynamodb.Table(scope, 'ServiceDiscoveryTable', {
    partitionKey: {
      name: 'service_id',
      type: dynamodb.AttributeType.STRING,
    },
    removalPolicy: cdk.RemovalPolicy.DESTROY,
    billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
    tableName: 'fractalbits-service-discovery',
  });
};

export const createInstance = (
  scope: Construct,
  vpc: ec2.Vpc,
  id: string,
  subnetType: ec2.SubnetType,
  instanceType: ec2.InstanceType,
  sg: ec2.SecurityGroup,
  role: iam.Role,
): ec2.Instance => {
  return new ec2.Instance(scope, id, {
    vpc: vpc,
    instanceType: instanceType,
    machineImage: ec2.MachineImage.latestAmazonLinux2023({
      cpuType: instanceType.architecture === ec2.InstanceArchitecture.ARM_64
        ? ec2.AmazonLinuxCpuType.ARM_64
        : ec2.AmazonLinuxCpuType.X86_64
    }),
    vpcSubnets: {subnetType},
    securityGroup: sg,
    role: role,
  });
};

export const createUserData = (scope: Construct, bootstrapOptions: string): ec2.UserData => {
  const region = cdk.Stack.of(scope).region;
  const userData = ec2.UserData.forLinux();
  userData.addCommands(
    'set -ex',
    `aws s3 cp --no-progress s3://fractalbits-builds-${region}/$(arch)/fractalbits-bootstrap /opt/fractalbits/bin/`,
    'chmod +x /opt/fractalbits/bin/fractalbits-bootstrap',
    `/opt/fractalbits/bin/fractalbits-bootstrap ${bootstrapOptions}`,
  );
  return userData;
};

export const createEc2Asg = (
  scope: Construct,
  id: string,
  vpc: ec2.Vpc,
  sg: ec2.SecurityGroup,
  role: iam.Role,
  instanceTypeNames: string[],
  bootstrapOptions: string,
  minCapacity: number,
  maxCapacity: number,
): autoscaling.AutoScalingGroup => {
  if (instanceTypeNames.length === 0) {
    throw new Error("instanceTypeNames must not be empty.");
  }

  const isArmInstance = (name: string): boolean => {
    const family = name.split('.')[0];
    // Graviton (arm) instances have a 'g' after the generation number, e.g. m6g, t4g, c7g.
    // The 'a1' family is also arm.
    // G-family instances (e.g. g4dn) are for GPU, not graviton, and are x86, but g5g is arm.
    return family === 'a1' || /\d[g]/.test(family);
  };

  const isArm = instanceTypeNames.every(isArmInstance);
  const isX86 = instanceTypeNames.every(name => !isArmInstance(name));
  if (!isArm && !isX86) {
    console.error("Error: both x86 and arm instance types are found, which is not supported for now.");
    process.exit(1);
  }

  const cpuType = isArm ? ec2.AmazonLinuxCpuType.ARM_64 : ec2.AmazonLinuxCpuType.X86_64;
  const launchTemplate = new ec2.LaunchTemplate(scope, `${id}Template`, {
    instanceType: new ec2.InstanceType(instanceTypeNames[0]),
    machineImage: ec2.MachineImage.latestAmazonLinux2023({cpuType}),
    securityGroup: sg,
    role: role,
    userData: createUserData(scope, bootstrapOptions),
  });
  const launchTemplateOverrides = instanceTypeNames.map(typeName => ({
    instanceType: new ec2.InstanceType(typeName),
  }));

  return new autoscaling.AutoScalingGroup(scope, id, {
    vpc: vpc,
    minCapacity: minCapacity,
    maxCapacity: maxCapacity,
    vpcSubnets: {
      subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
    },
    newInstancesProtectedFromScaleIn: false,
    mixedInstancesPolicy: {
      instancesDistribution: {
        onDemandPercentageAboveBaseCapacity: 100,
      },
      launchTemplate: launchTemplate,
      launchTemplateOverrides: launchTemplateOverrides,
    },
  });
};

export const createEbsVolume = (
  scope: Construct,
  id: string,
  az: string,
  instanceId: string,
): ec2.Volume => {
  const ebsVolume = new ec2.Volume(scope, id, {
    removalPolicy: cdk.RemovalPolicy.DESTROY,
    availabilityZone: az,
    size: cdk.Size.gibibytes(20),
    volumeType: ec2.EbsDeviceVolumeType.IO2,
    iops: 10000,
    enableMultiAttach: true,
  });

  new ec2.CfnVolumeAttachment(scope, `${id}Attachment`, {
    instanceId: instanceId,
    device: '/dev/xvdf',
    volumeId: ebsVolume.volumeId,
  });

  return ebsVolume;
};

export function addAsgDeregistrationLifecycleHook(
  scope: Construct,
  id: string,
  asg: autoscaling.AutoScalingGroup,
  service: servicediscovery.Service,
) {
  const deregisterLambdaRole = new iam.Role(scope, `${id}DeregisterRole`, {
    assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    managedPolicies: [
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      iam.ManagedPolicy.fromAwsManagedPolicyName('AWSCloudMapFullAccess'),
    ],
  });

  deregisterLambdaRole.addToPolicy(new iam.PolicyStatement({
    actions: ['autoscaling:CompleteLifecycleAction'],
    resources: [asg.autoScalingGroupArn],
  }));

  const deregisterInstanceLambda = new lambda.Function(scope, `${id}DeregisterInstanceLifecycleLambda`, {
    runtime: lambda.Runtime.NODEJS_20_X,
    handler: 'index.handler',
    code: lambda.Code.fromAsset(path.join(__dirname, 'lambda/deregister-instance-lifecycle')),
    environment: {
      SERVICE_ID: service.serviceId,
    },
    role: deregisterLambdaRole,
  });

  new autoscaling.LifecycleHook(scope, `${id}LifecycleHook`, {
    autoScalingGroup: asg,
    lifecycleTransition: autoscaling.LifecycleTransition.INSTANCE_TERMINATING,
    heartbeatTimeout: cdk.Duration.minutes(5),
    notificationTarget: new hooktargets.FunctionHook(deregisterInstanceLambda),
  });
}

