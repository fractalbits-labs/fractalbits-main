import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';


export class FractalbitsMetaStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, 'FractalbitsMetaStackVpc', {
      vpcName: 'fractalbits-meta-stack-vpc',
      maxAzs: 1,
      natGateways: 0,
      subnetConfiguration: [
        { name: 'PublicSubnet', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 },
        { name: 'PrivateSubnet', subnetType: ec2.SubnetType.PRIVATE_ISOLATED, cidrMask: 24 },
      ],
    });

    // Add Gateway Endpoint for S3
    vpc.addGatewayEndpoint('S3Endpoint', {
      service: ec2.GatewayVpcEndpointAwsService.S3,
    });

    // Add Interface Endpoint for EC2 and SSM
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


    // Reusable functions to create instances
    const createInstance = (
      id: string,
      subnetType: ec2.SubnetType,
      instanceType: ec2.InstanceType,
    ): ec2.Instance => {
      return new ec2.Instance(this, id, {
        vpc,
        instanceType: instanceType,
        machineImage: ec2.MachineImage.latestAmazonLinux2023({
          cpuType: ec2.AmazonLinuxCpuType.ARM_64,
        }),
        vpcSubnets: { subnetType },
        securityGroup: sg,
        role: ec2Role,
      });
    };
    const createUserData = (cpuArch: string, bootstrapOptions: string): ec2.UserData => {
      const region = cdk.Stack.of(this).region;
      const userData = ec2.UserData.forLinux();
      userData.addCommands(
        'set -ex',
        `aws s3 cp --no-progress s3://fractalbits-builds-${region}/${cpuArch}/fractalbits-bootstrap /opt/fractalbits/bin/`,
        'chmod +x /opt/fractalbits/bin/fractalbits-bootstrap',
        `/opt/fractalbits/bin/fractalbits-bootstrap ${bootstrapOptions}`,
      );
      return userData;
    };

    const nssInstanceType = ec2.InstanceType.of(ec2.InstanceClass.M7GD, ec2.InstanceSize.XLARGE4);
    const cpuArch = "aarch64";
    const nssBootstrapOptions = `nss_bench --num_nvme_disks=1`;
    let nssInstance = createInstance("nss_bench", ec2.SubnetType.PRIVATE_ISOLATED, nssInstanceType);
    nssInstance.addUserData(createUserData(cpuArch, nssBootstrapOptions).render());

    new cdk.CfnOutput(this, 'nssBenchId', {
      value: nssInstance.instanceId,
      description: `EC2 nss instance ID`,
    });
  }
}
