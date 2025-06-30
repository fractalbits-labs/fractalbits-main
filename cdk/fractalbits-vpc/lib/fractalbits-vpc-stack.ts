import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as elbv2 from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import * as elbv2_targets from 'aws-cdk-lib/aws-elasticloadbalancingv2-targets';
import { createInstance, createUserData } from './ec2-utils';

export class FractalbitsVpcStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // === VPC Configuration ===
    this.vpc = new ec2.Vpc(this, 'FractalbitsVpc', {
      vpcName: 'fractalbits-vpc',
      ipAddresses: ec2.IpAddresses.cidr('10.0.0.0/16'),
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

    // Add Gateway Endpoint for DynamoDB
    this.vpc.addGatewayEndpoint('DynamoDbEndpoint', {
      service: ec2.GatewayVpcEndpointAwsService.DYNAMODB,
    });

    // Add Interface Endpoint for EC2 and SSM
    ['SSM', 'SSM_MESSAGES', 'EC2', 'EC2_MESSAGES'].forEach(service => {
      this.vpc.addInterfaceEndpoint(`${service}Endpoint`, {
        service: (ec2.InterfaceVpcEndpointAwsService as any)[service],
        subnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      });
    });

    // IAM Role for EC2
    const ec2Role = new iam.Role(this, 'InstanceRole', {
      roleName: 'FractalbitsInstanceRole',
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMFullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonDynamoDBFullAccess_v2'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonEC2FullAccess'),
      ],
    });

    const publicSg = new ec2.SecurityGroup(this, 'PublicInstanceSG', {
      vpc: this.vpc,
      securityGroupName: 'FractalbitsPublicInstanceSG',
      description: 'Allow inbound on port 80 for public access, and outbound for SSM, DDB, S3',
      allowAllOutbound: true,
    });
    publicSg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(80), 'Allow HTTP access from anywhere');

    const privateSg = new ec2.SecurityGroup(this, 'PrivateInstanceSG', {
      vpc: this.vpc,
      securityGroupName: 'FractalbitsPrivateInstanceSG',
      description: 'Allow inbound on port 8088 (e.g., from internal sources), and outbound for SSM, DDB, S3',
      allowAllOutbound: true,
    });
    privateSg.addIngressRule(ec2.Peer.ipv4(this.vpc.vpcCidrBlock), ec2.Port.tcp(8088), 'Allow access to port 8088 from within VPC');

    const bucket = new s3.Bucket(this, 'Bucket', {
      // No bucketName provided – name will be auto-generated
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Delete bucket on stack delete
      autoDeleteObjects: true,                  // Empty bucket before deletion
    });

    new dynamodb.Table(this, 'FractalbitsTable', {
      partitionKey: {
        name: 'id',
        type: dynamodb.AttributeType.STRING,
      },
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Delete table on stack delete
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      tableName: 'fractalbits-keys-and-buckets',
    });

    new dynamodb.Table(this, 'EBSFailoverStateTable', {
      partitionKey: {
        name: 'VolumeId',
        type: dynamodb.AttributeType.STRING,
      },
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Delete table on stack delete
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      tableName: 'ebs-failover-state',
    });

    // Define instance metadata
    const nssInstanceType = ec2.InstanceType.of(ec2.InstanceClass.M7GD, ec2.InstanceSize.XLARGE4);
    const bssInstanceType = ec2.InstanceType.of(ec2.InstanceClass.M7GD, ec2.InstanceSize.XLARGE4);
    const rssInstanceType = ec2.InstanceType.of(ec2.InstanceClass.C7G, ec2.InstanceSize.MEDIUM);
    const apiInstanceType = ec2.InstanceType.of(ec2.InstanceClass.C7G, ec2.InstanceSize.LARGE);
    const nssNumNvmeDisks = 1;
    const bssNumNvmeDisks = 1;
    const bucketName = bucket.bucketName;
    const instanceConfigs = [
      { id: 'api_server_1', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: apiInstanceType, sg: publicSg},
      { id: 'api_server_2', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: apiInstanceType, sg: publicSg},
      { id: 'root_server', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: rssInstanceType, sg: privateSg },
      { id: 'bss_server', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: bssInstanceType, sg: privateSg },
      { id: 'nss_server_primary', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: nssInstanceType, sg: privateSg },
      // { id: 'nss_server_secondary', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: nss_instance_type, sg: privateSg },
    ];

    const instances: Record<string, ec2.Instance> = {};
    instanceConfigs.forEach(({ id, subnet, instanceType, sg}) => {
      instances[id] = createInstance(this, this.vpc, id, subnet, instanceType, sg, ec2Role);
    });

    // NLB for API servers
    const nlb = new elbv2.NetworkLoadBalancer(this, 'ApiNLB', {
      vpc: this.vpc,
      internetFacing: false,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
    });

    const listener = nlb.addListener('ApiListener', { port: 80 });

    listener.addTargets('ApiTargets', {
      port: 80,
      targets: [
        new elbv2_targets.InstanceTarget(instances['api_server_1']),
        new elbv2_targets.InstanceTarget(instances['api_server_2']),
      ],
    });

    // Create EBS Volume with Multi-Attach for nss_server
    const ebsVolume = new ec2.Volume(this, 'MultiAttachVolume', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      availabilityZone: this.vpc.availabilityZones[0],
      size: cdk.Size.gibibytes(20),
      volumeType: ec2.EbsDeviceVolumeType.IO2,
      iops: 10000,
      enableMultiAttach: true,
    });
    // Attach volume to primary nss_server instance
    new ec2.CfnVolumeAttachment(this, 'AttachVolumeToActive', {
      instanceId: instances['nss_server_primary'].instanceId,
      device: '/dev/xvdf',
      volumeId: ebsVolume.volumeId,
    });

    // Create UserData: we need to make it a separate step since we want to get the instance/volume ids
    const primaryNss = instances['nss_server_primary'].instanceId;
    const secondaryNss = instances['nss_server_secondary']?.instanceId ?? null;
    const ebsVolumeId = ebsVolume.volumeId;
    const bssIp = instances["bss_server"].instancePrivateIp;
    const nssIp = instances["nss_server_primary"].instancePrivateIp;
    const rssIp = instances["root_server"].instancePrivateIp;
    const cpuArch = "aarch64";
    const instanceBootstrapOptions = [
      {
        id: 'api_server_1',
        bootstrapOptions: `api_server --bucket=${bucketName} --bss_ip=${bssIp} --nss_ip=${nssIp} --rss_ip=${rssIp}`
      },
      {
        id: 'api_server_2',
        bootstrapOptions: `api_server --bucket=${bucketName} --bss_ip=${bssIp} --nss_ip=${nssIp} --rss_ip=${rssIp}`
      },
      {
        id: 'root_server',
        bootstrapOptions: `root_server --primary_instance_id=${primaryNss} --secondary_instance_id=${secondaryNss} --volume_id=${ebsVolumeId}`
      },
      {
        id: 'bss_server',
        bootstrapOptions: `bss_server --num_nvme_disks=${bssNumNvmeDisks}` },
      {
        id: 'nss_server_primary',
        bootstrapOptions: `nss_server --bucket=${bucketName} --volume_id=${ebsVolumeId} --num_nvme_disks=${nssNumNvmeDisks}`
      },
      {
        id: 'nss_server_secondary',
        bootstrapOptions: `nss_server --bucket=${bucketName} --volume_id=${ebsVolumeId} --num_nvme_disks=${nssNumNvmeDisks}`
      },
    ];
    instanceBootstrapOptions.forEach(({id, bootstrapOptions}) => {
      instances[id]?.addUserData(createUserData(this, cpuArch, bootstrapOptions).render())
    })

    // Outputs
    new cdk.CfnOutput(this, 'FractalbitsBucketName', {
      value: bucket.bucketName,
    });

    for (const [id, instance] of Object.entries(instances)) {
      new cdk.CfnOutput(this, `${id}Id`, {
        value: instance.instanceId,
        description: `EC2 instance ${id} ID`,
      });
    }

    new cdk.CfnOutput(this, 'ApiNLBDnsName', {
      value: nlb.loadBalancerDnsName,
      description: 'DNS name of the API NLB',
    });

    new cdk.CfnOutput(this, 'VolumeId', {
      value: ebsVolumeId,
      description: 'EBS volume ID',
    });
  }
}
