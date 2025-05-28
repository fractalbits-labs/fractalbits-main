import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as autoscaling from 'aws-cdk-lib/aws-autoscaling';

export class FractalbitsVpcStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create the VPC with 1 AZ and custom subnet configuration
    const vpc = new ec2.Vpc(this, 'FractalbitsVpc', {
      vpcName: 'fractalbits-vpc',
      maxAzs: 1,
      natGateways: 0,
      subnetConfiguration: [
        {
          name: 'PublicSubnet',
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
        {
          name: 'PrivateSubnet',
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
          cidrMask: 24,
        },
      ],
    });

    // Add S3 VPC endpoint
    vpc.addGatewayEndpoint('S3Endpoint', {
      service: ec2.GatewayVpcEndpointAwsService.S3,
      subnets: [
        { subnetType: ec2.SubnetType.PUBLIC },
        { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      ],
    });

    // Add SSM VPC Endpoints (required for private subnet)
    vpc.addInterfaceEndpoint('SSMEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.SSM,
      subnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
    });

    vpc.addInterfaceEndpoint('SSMMessagesEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.SSM_MESSAGES,
      subnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
    });

    vpc.addInterfaceEndpoint('EC2MessagesEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.EC2_MESSAGES,
      subnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
    });

    // IAM role for EC2 with SSM + S3
    const ec2Role = new iam.Role(this, 'InstanceRole', {
      roleName: 'FractalbitsInstanceRole',
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
    });
    ec2Role.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore')
    );
    ec2Role.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess')
    );

    // Security Group (no inbound rules; all outbound allowed)
    const sg = new ec2.SecurityGroup(this, 'InstanceSG', {
      vpc,
      securityGroupName: 'FractalbitsInstanceSG',
      description: 'Allow outbound only for SSM and S3 access',
      allowAllOutbound: true,
    });

    // Amazon Linux 2023 AMI
    const ami = ec2.MachineImage.latestAmazonLinux2023();

    const apiServerInstance = new ec2.Instance(this, 'api_server', {
      vpc,
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T2, ec2.InstanceSize.MICRO),
      machineImage: ami,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      securityGroup: sg,
      role: ec2Role,
    });

    const rootServerInstance = new ec2.Instance(this, 'root_server', {
      vpc,
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T2, ec2.InstanceSize.MICRO),
      machineImage: ami,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      securityGroup: sg,
      role: ec2Role,
    });

    const bssServerInstance = new ec2.Instance(this, 'bss_server', {
      vpc,
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T2, ec2.InstanceSize.MICRO),
      machineImage: ami,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      securityGroup: sg,
      role: ec2Role,
    });

    const nssLaunchTemplate = new ec2.LaunchTemplate(this, 'NssLaunchTemplate', {
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T2, ec2.InstanceSize.MICRO),
      machineImage: ami,
      securityGroup: sg,
      role: ec2Role,
      httpTokens: ec2.LaunchTemplateHttpTokens.OPTIONAL,
      httpEndpoint: true,
    });
    const nssASG = new autoscaling.AutoScalingGroup(this, 'nssASG', {
      vpc,
      minCapacity: 1,
      maxCapacity: 1,
      desiredCapacity: 1,
      launchTemplate: nssLaunchTemplate,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
    });
    cdk.Tags.of(nssASG).add('Name', `${cdk.Stack.of(this).stackName}/nss_server/ASG`);

    // Outputs
    new cdk.CfnOutput(this, 'ApiServerId', {
      value: apiServerInstance.instanceId,
      description: 'EC2 instance API server ID',
    });

    new cdk.CfnOutput(this, 'RootServerId', {
      value: rootServerInstance.instanceId,
      description: 'EC2 instance root server ID',
    });

    new cdk.CfnOutput(this, 'BssServerId', {
      value: bssServerInstance.instanceId,
      description: 'EC2 instance bss server ID',
    });
  }
}

