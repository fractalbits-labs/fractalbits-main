import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as autoscaling from 'aws-cdk-lib/aws-autoscaling';
import {Construct} from 'constructs';
import * as servicediscovery from 'aws-cdk-lib/aws-servicediscovery';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as cr from 'aws-cdk-lib/custom-resources';

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
  const x86InstanceTypes: string[] = [];
  const armInstanceTypes: string[] = [];

  instanceTypeNames.forEach(typeName => {
    if (typeName.includes('g')) { // Simple heuristic for Graviton (ARM) instances
      armInstanceTypes.push(typeName);
    } else {
      x86InstanceTypes.push(typeName);
    }
  });
  if (x86InstanceTypes.length > 0 && armInstanceTypes.length > 0) {
    console.error("Error: both x86 and arm instance types are found, which is not supported for now.");
    process.exit(1);
  }

  const launchTemplateOverrides: autoscaling.LaunchTemplateOverrides[] = [];

  let x86LaunchTemplate: ec2.LaunchTemplate | undefined;
  if (x86InstanceTypes.length > 0) {
    x86LaunchTemplate = new ec2.LaunchTemplate(scope, `${id}X86LaunchTemplate`, {
      instanceType: new ec2.InstanceType(x86InstanceTypes[0]),
      machineImage: ec2.MachineImage.latestAmazonLinux2023({cpuType: ec2.AmazonLinuxCpuType.X86_64}),
      securityGroup: sg,
      role: role,
      userData: createUserData(scope, bootstrapOptions),
    });
    x86InstanceTypes.slice(1).forEach(typeName => {
      launchTemplateOverrides.push({
        instanceType: new ec2.InstanceType(typeName),
      });
    });
  }

  let armLaunchTemplate: ec2.LaunchTemplate | undefined;
  if (armInstanceTypes.length > 0) {
    armLaunchTemplate = new ec2.LaunchTemplate(scope, `${id}ArmLaunchTemplate`, {
      instanceType: new ec2.InstanceType(armInstanceTypes[0]),
      machineImage: ec2.MachineImage.latestAmazonLinux2023({cpuType: ec2.AmazonLinuxCpuType.ARM_64}),
      securityGroup: sg,
      role: role,
      userData: createUserData(scope, bootstrapOptions),
    });
    armInstanceTypes.forEach(typeName => {
      launchTemplateOverrides.push({
        instanceType: new ec2.InstanceType(typeName),
      });
    });
  }

  const defaultLaunchTemplate = x86LaunchTemplate || armLaunchTemplate;

  if (!defaultLaunchTemplate) {
    throw new Error("No valid launch template could be created. Ensure instanceTypeNames are valid.");
  }

  return new autoscaling.AutoScalingGroup(scope, id, {
    vpc: vpc,
    minCapacity: minCapacity,
    maxCapacity: maxCapacity,
    desiredCapacity: maxCapacity,
    vpcSubnets: {
      subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
    },
    newInstancesProtectedFromScaleIn: false,
    mixedInstancesPolicy: {
      instancesDistribution: {
        onDemandPercentageAboveBaseCapacity: 100,
      },
      launchTemplate: defaultLaunchTemplate,
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

export const setupAsgCloudMapDeregistration = (
  scope: Construct,
  id: string,
  service: servicediscovery.Service,
  namespace: servicediscovery.PrivateDnsNamespace,
  asg: autoscaling.AutoScalingGroup
) => {
  const stack = cdk.Stack.of(scope);

  let deregisterLambdaRole = stack.node.tryFindChild('DeregisterLambdaRole') as iam.Role;
  if (!deregisterLambdaRole) {
    deregisterLambdaRole = new iam.Role(scope, 'DeregisterLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AWSCloudMapFullAccess'),
      ],
    });
  }

  let deregisterLambda = stack.node.tryFindChild('DeregisterLambda') as lambda.Function;
  if (!deregisterLambda) {
    deregisterLambda = new lambda.Function(scope, 'DeregisterLambda', {
      runtime: lambda.Runtime.NODEJS_18_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
        const { ServiceDiscoveryClient, DeregisterInstanceCommand, DiscoverInstancesCommand } = require('@aws-sdk/client-servicediscovery');
        const servicediscovery = new ServiceDiscoveryClient({});

        exports.handler = async (event, context) => {
          const serviceId = event.ResourceProperties.ServiceId;
          const namespaceName = event.ResourceProperties.NamespaceName;
          const serviceName = event.ResourceProperties.ServiceName;

          if (event.RequestType === 'Delete') {
            try {
              const discoverInstancesCommand = new DiscoverInstancesCommand({ NamespaceName: namespaceName, ServiceName: serviceName });
              const instances = await servicediscovery.send(discoverInstancesCommand);

              for (const instance of instances.Instances) {
                const deregisterInstanceCommand = new DeregisterInstanceCommand({ ServiceId: serviceId, InstanceId: instance.InstanceId });
                await servicediscovery.send(deregisterInstanceCommand);
              }
            } catch (error) {
              console.error('Error deregistering instances:', error);
              // Don't fail the custom resource on error, as the service might already be gone
            }
          }

          return { Status: 'SUCCESS' };
        };
      `),
      role: deregisterLambdaRole,
    });
  }

  let deregisterProvider = stack.node.tryFindChild('DeregisterProvider') as cr.Provider;
  if (!deregisterProvider) {
    deregisterProvider = new cr.Provider(scope, 'DeregisterProvider', {
      onEventHandler: deregisterLambda,
    });
  }

  const resource = new cdk.CustomResource(scope, id, {
    serviceToken: deregisterProvider.serviceToken,
    properties: {
      ServiceId: service.serviceId,
      NamespaceName: namespace.namespaceName,
      ServiceName: service.serviceName,
    },
  });
  resource.node.addDependency(asg);
};
