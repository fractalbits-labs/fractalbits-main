import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';

export class FractalbitsVpcStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // === VPC Configuration ===
    const vpc = new ec2.Vpc(this, 'FractalbitsVpc', {
      vpcName: 'fractalbits-vpc',
      maxAzs: 1,
      natGateways: 0,
      subnetConfiguration: [
        { name: 'PublicSubnet', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 },
        { name: 'PrivateSubnet', subnetType: ec2.SubnetType.PRIVATE_ISOLATED, cidrMask: 24 },
      ],
    });

    // S3 & SSM VPC Endpoints
    vpc.addGatewayEndpoint('S3Endpoint', {
      service: ec2.GatewayVpcEndpointAwsService.S3,
      subnets: [{ subnetType: ec2.SubnetType.PUBLIC }, { subnetType: ec2.SubnetType.PRIVATE_ISOLATED }],
    });

    ['SSM', 'SSM_MESSAGES', 'EC2_MESSAGES'].forEach(service => {
      vpc.addInterfaceEndpoint(`${service}Endpoint`, {
        service: (ec2.InterfaceVpcEndpointAwsService as any)[service],
        subnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      });
    });

    // IAM Role for EC2
    const ec2Role = new iam.Role(this, 'InstanceRole', {
      roleName: 'FractalbitsInstanceRole',
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
      ],
    });

    // Security Group
    const sg = new ec2.SecurityGroup(this, 'InstanceSG', {
      vpc,
      securityGroupName: 'FractalbitsInstanceSG',
      description: 'Allow outbound only for SSM and S3 access',
      allowAllOutbound: true,
    });

    [8888, 9224, 9225, 3000].forEach(port => {
      sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(port), `Allow port ${port}`);
    });

    // Reusable function to create UserData
    const createUserData = (serverName: string): ec2.UserData => {
      const userData = ec2.UserData.forLinux();
      userData.addCommands(
        'set -e',
        'aws s3 cp --no-progress s3://fractalbits-builds/fractalbits-bootstrap /opt/fractalbits/bin/',
        'chmod -v +x /opt/fractalbits/bin/fractalbits-bootstrap',
        `/opt/fractalbits/bin/fractalbits-bootstrap ${serverName}`,
      );
      return userData;
    };

    // Reusable function to create instances
    const createInstance = (
      id: string,
      privateIp: string,
      subnetType: ec2.SubnetType
    ): ec2.Instance => {
      return new ec2.Instance(this, id, {
        vpc,
        instanceType: ec2.InstanceType.of(ec2.InstanceClass.T2, ec2.InstanceSize.MICRO),
        machineImage: ec2.MachineImage.latestAmazonLinux2023(),
        vpcSubnets: { subnetType },
        securityGroup: sg,
        role: ec2Role,
        privateIpAddress: privateIp,
        userData: createUserData(id),
      });
    };

    // Define instance metadata
    const instanceConfigs = [
      { id: 'api_server', ip: '10.0.0.11', subnet: ec2.SubnetType.PUBLIC },
      { id: 'root_server', ip: '10.0.1.254', subnet: ec2.SubnetType.PRIVATE_ISOLATED },
      { id: 'bss_server', ip: '10.0.1.10', subnet: ec2.SubnetType.PRIVATE_ISOLATED },
      { id: 'nss_server', ip: '10.0.1.100', subnet: ec2.SubnetType.PRIVATE_ISOLATED },
    ];

    const instances: Record<string, ec2.Instance> = {};

    instanceConfigs.forEach(({ id, ip, subnet }) => {
      instances[id] = createInstance(id, ip, subnet);
    });

    // Outputs
    for (const [id, instance] of Object.entries(instances)) {
      new cdk.CfnOutput(this, `${id}Id`, {
        value: instance.instanceId,
        description: `EC2 instance ${id} ID`,
      });
    }

    new cdk.CfnOutput(this, 'ServicePublicIP', {
      value: instances['api_server'].instancePublicIp,
      description: 'Public IP of the API server',
    });
  }
}
