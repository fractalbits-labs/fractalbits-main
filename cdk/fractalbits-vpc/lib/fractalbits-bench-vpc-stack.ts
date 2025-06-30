import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import { createInstance, createUserData } from './ec2-utils';

interface FractalbitsBenchVpcStackProps extends cdk.StackProps {
  serviceEndpoint: string;
}

export class FractalbitsBenchVpcStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;

  constructor(scope: Construct, id: string, props: FractalbitsBenchVpcStackProps) {
    super(scope, id, props);

    // === VPC Configuration ===
    this.vpc = new ec2.Vpc(this, 'FractalbitsBenchVpc', {
      vpcName: 'fractalbits-bench-vpc',
      ipAddresses: ec2.IpAddresses.cidr('10.1.0.0/16'),
      maxAzs: 1,
      natGateways: 0,
      subnetConfiguration: [
        { name: 'PrivateSubnet', subnetType: ec2.SubnetType.PRIVATE_ISOLATED, cidrMask: 24 },
      ],
    });

    // Add Gateway Endpoint for S3
    this.vpc.addGatewayEndpoint('S3Endpoint', {
      service: ec2.GatewayVpcEndpointAwsService.S3,
    });

    // Add Interface Endpoint for EC2 and SSM
    ['SSM', 'SSM_MESSAGES', 'EC2', 'EC2_MESSAGES'].forEach(service => {
      this.vpc.addInterfaceEndpoint(`${service}Endpoint`, {
        service: (ec2.InterfaceVpcEndpointAwsService as any)[service],
        subnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      });
    });

    // IAM Role for EC2
    const ec2Role = new iam.Role(this, 'BenchInstanceRole', {
      roleName: 'FractalbitsBenchInstanceRole',
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMFullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonEC2FullAccess'),
      ],
    });

    const privateSg = new ec2.SecurityGroup(this, 'BenchPrivateInstanceSG', {
      vpc: this.vpc,
      securityGroupName: 'FractalbitsBenchPrivateInstanceSG',
      description: 'Allow outbound for SSM',
      allowAllOutbound: true,
    });

    // EC2 Instance
    const instance = createInstance(this, this.vpc, 'BenchInstance', ec2.SubnetType.PRIVATE_ISOLATED, ec2.InstanceType.of(ec2.InstanceClass.C7G, ec2.InstanceSize.MEDIUM), privateSg, ec2Role);

    const cpuArch = "aarch64";
    const bootstrapOptions = `bench_server --service_endpoint=${props.serviceEndpoint}`;
    instance.addUserData(createUserData(this, cpuArch, bootstrapOptions).render());

    // Outputs
    new cdk.CfnOutput(this, 'BenchInstanceId', {
      value: instance.instanceId,
      description: 'EC2 instance ID for the bench stack',
    });
  }
}
