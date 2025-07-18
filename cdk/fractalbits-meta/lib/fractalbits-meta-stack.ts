import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';

interface FractalbitsMetaStackProps extends cdk.StackProps {
  serviceName: string;
  bssUseI3?: boolean;
}

export class FractalbitsMetaStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: FractalbitsMetaStackProps) {
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
      const instance = new ec2.Instance(this, id, {
        vpc,
        instanceType: instanceType,
        machineImage: ec2.MachineImage.latestAmazonLinux2023({
          cpuType: instanceType.architecture === ec2.InstanceArchitecture.ARM_64
            ? ec2.AmazonLinuxCpuType.ARM_64
            : ec2.AmazonLinuxCpuType.X86_64
        }),
        vpcSubnets: { subnetType },
        securityGroup: sg,
        role: ec2Role,
      });

      return instance;
    };
    const createUserData = (bootstrapOptions: string): ec2.UserData => {
      const region = cdk.Stack.of(this).region;
      const userData = ec2.UserData.forLinux();
      userData.addCommands(
        'set -ex',
        `aws s3 cp --no-progress s3://fractalbits-builds-${region}/$(arch)/fractalbits-bootstrap /opt/fractalbits/bin/`,
        'chmod +x /opt/fractalbits/bin/fractalbits-bootstrap',
        `/opt/fractalbits/bin/fractalbits-bootstrap ${bootstrapOptions}`,
      );
      return userData;
    };
    let instance = undefined;
    if (props.serviceName == "nss") {
      const nssInstanceType = ec2.InstanceType.of(ec2.InstanceClass.M7GD, ec2.InstanceSize.XLARGE4);
      instance = createInstance(`${props.serviceName}_bench`, ec2.SubnetType.PRIVATE_ISOLATED, nssInstanceType);
      // Create EBS Volume with Multi-Attach capabilities
      const ebsVolume = new ec2.Volume(this, 'MultiAttachVolume', {
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        availabilityZone: vpc.availabilityZones[0],
        size: cdk.Size.gibibytes(20),
        volumeType: ec2.EbsDeviceVolumeType.IO2,
        iops: 10000,
        enableMultiAttach: true,
      });
      const bucket = new s3.Bucket(this, 'Bucket', {
        // No bucketName provided – name will be auto-generated
        removalPolicy: cdk.RemovalPolicy.DESTROY, // Delete bucket on stack delete
        autoDeleteObjects: true,                  // Empty bucket before deletion
      });

      const nssBootstrapOptions = `nss_server --bucket=${bucket.bucketName} --volume_id=${ebsVolume.volumeId} --num_nvme_disks=1 --meta_stack_testing`;
      instance.addUserData(createUserData(nssBootstrapOptions).render());

      // Attach volume
      new ec2.CfnVolumeAttachment(this, 'AttachVolumeToActive', {
        instanceId: instance.instanceId,
        device: '/dev/xvdf',
        volumeId: ebsVolume.volumeId,
      });
    } else {
      const bssInstanceType = props.bssUseI3
          ? ec2.InstanceType.of(ec2.InstanceClass.I3, ec2.InstanceSize.METAL)
          : ec2.InstanceType.of(ec2.InstanceClass.IS4GEN, ec2.InstanceSize.XLARGE);
      const bssNumNvmeDisks = props.bssUseI3 ? 8 : 1;
      instance = createInstance(`${props.serviceName}_bench`, ec2.SubnetType.PRIVATE_ISOLATED, bssInstanceType);
      instance.addUserData(createUserData(`bss_server --num_nvme_disks=${bssNumNvmeDisks} --meta_stack_testing`).render());
    }

    new cdk.CfnOutput(this, 'instanceId', {
      value: instance.instanceId,
      description: `EC2 instance ID`,
    });
  }
}
