import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

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

    // Add Gateway Endpoint for S3
    vpc.addGatewayEndpoint('S3Endpoint', {
      service: ec2.GatewayVpcEndpointAwsService.S3,
    });

    // Add Gateway Endpoint for DynamoDB
    vpc.addGatewayEndpoint('DynamoDbEndpoint', {
      service: ec2.GatewayVpcEndpointAwsService.DYNAMODB,
    });

    // Add Interface Endpoint for EC2 and SSM
    ['SSM', 'SSM_MESSAGES', 'EC2', 'EC2_MESSAGES'].forEach(service => {
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
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMFullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonDynamoDBFullAccess_v2'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonEC2FullAccess'),
      ],
    });

    // Security Group
    const sg = new ec2.SecurityGroup(this, 'InstanceSG', {
      vpc,
      securityGroupName: 'FractalbitsInstanceSG',
      description: 'Allow outbound only for SSM, DDB and S3 access',
      allowAllOutbound: true,
    });

    [8888, 9224, 9225, 3000].forEach(port => {
      sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(port), `Allow port ${port}`);
    });

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

    // Define instance metadata
    const t4gSmall = ec2.InstanceType.of(ec2.InstanceClass.T4G, ec2.InstanceSize.SMALL);
    const nssInstanceType = ec2.InstanceType.of(ec2.InstanceClass.M7GD, ec2.InstanceSize.XLARGE4);
    const nssNumNvmeDisks = 1;
    const bucketName = bucket.bucketName;
    const instanceConfigs = [
      { id: 'api_server', subnet: ec2.SubnetType.PUBLIC, instanceType: t4gSmall },
      { id: 'root_server', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: t4gSmall },
      { id: 'bss_server', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: t4gSmall },
      { id: 'nss_server_primary', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: nssInstanceType },
      // { id: 'nss_server_secondary', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: nss_instance_type },
    ];

    const instances: Record<string, ec2.Instance> = {};
    instanceConfigs.forEach(({ id, subnet, instanceType}) => {
      instances[id] = createInstance(id, subnet, instanceType);
    });

    // Create EBS Volume with Multi-Attach for nss_server
    const ebsVolume = new ec2.Volume(this, 'MultiAttachVolume', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      availabilityZone: vpc.availabilityZones[0],
      size: cdk.Size.gibibytes(10),
      volumeType: ec2.EbsDeviceVolumeType.IO2,
      iops: 100,
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
        id: 'api_server',
        bootstrapOptions: `api_server --bucket=${bucketName} --bss_ip=${bssIp} --nss_ip=${nssIp} --rss_ip=${rssIp}`
      },
      {
        id: 'root_server',
        bootstrapOptions: `root_server --primary_instance_id=${primaryNss} --secondary_instance_id=${secondaryNss} --volume_id=${ebsVolumeId}`
      },
      {
        id: 'bss_server',
        bootstrapOptions: `bss_server` },
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
      instances[id]?.addUserData(createUserData(cpuArch, bootstrapOptions).render())
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

    new cdk.CfnOutput(this, 'ServicePublicIP', {
      value: instances['api_server'].instancePublicIp,
      description: 'Public IP of the API server',
    });

    new cdk.CfnOutput(this, 'VolumeId', {
      value: ebsVolumeId,
      description: 'EBS volume ID',
    });
  }
}
