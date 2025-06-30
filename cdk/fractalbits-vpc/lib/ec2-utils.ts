import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

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
      cpuType: ec2.AmazonLinuxCpuType.ARM_64,
    }),
    vpcSubnets: { subnetType },
    securityGroup: sg,
    role: role,
  });
};

export const createUserData = (scope: Construct, cpuArch: string, bootstrapOptions: string): ec2.UserData => {
  const region = cdk.Stack.of(scope).region;
  const userData = ec2.UserData.forLinux();
  userData.addCommands(
    'set -ex',
    `aws s3 cp --no-progress s3://fractalbits-builds-${region}/${cpuArch}/fractalbits-bootstrap /opt/fractalbits/bin/`,
    'chmod +x /opt/fractalbits/bin/fractalbits-bootstrap',
    `/opt/fractalbits/bin/fractalbits-bootstrap ${bootstrapOptions}`,
  );
  return userData;
};
