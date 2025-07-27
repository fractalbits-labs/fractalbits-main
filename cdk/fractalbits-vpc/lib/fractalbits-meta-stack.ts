import * as cdk from 'aws-cdk-lib';
import {Construct} from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as servicediscovery from 'aws-cdk-lib/aws-servicediscovery';
import {createEbsVolume, createEc2Asg, createInstance, createUserData, addAsgDeregistrationLifecycleHook} from './ec2-utils';

interface FractalbitsMetaStackProps extends cdk.StackProps {
  serviceName: string;
  availabilityZone?: string;
  nssInstanceType?: string;
  bssInstanceTypes?: string;
  deregisterProviderServiceToken: string;
}

export class FractalbitsMetaStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;

  constructor(scope: Construct, id: string, props: FractalbitsMetaStackProps) {
    super(scope, id, props);

    const az = props.availabilityZone ?? this.availabilityZones[this.availabilityZones.length - 1];
    this.vpc = new ec2.Vpc(this, 'FractalbitsMetaStackVpc', {
      vpcName: 'fractalbits-meta-stack-vpc',
      availabilityZones: [az],
      natGateways: 0,
      subnetConfiguration: [
        {name: 'PrivateSubnet', subnetType: ec2.SubnetType.PRIVATE_ISOLATED, cidrMask: 24},
      ],
    });

    // Add Gateway Endpoint for S3
    this.vpc.addGatewayEndpoint('S3Endpoint', {
      service: ec2.GatewayVpcEndpointAwsService.S3,
    });

    // Add Interface Endpoint for EC2, SSM and CloudMap
    ['SSM', 'SSM_MESSAGES', 'EC2_MESSAGES', 'CLOUD_MAP_SERVICE_DISCOVERY'].forEach(service => {
      this.vpc.addInterfaceEndpoint(`${service}Endpoint`, {
        service: (ec2.InterfaceVpcEndpointAwsService as any)[service],
        subnets: {subnetType: ec2.SubnetType.PRIVATE_ISOLATED},
      });
    });

    // IAM Role for EC2
    const ec2Role = new iam.Role(this, 'InstanceRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AWSCloudMapFullAccess'),
      ],
    });

    const privateDnsNamespace = new servicediscovery.PrivateDnsNamespace(this, 'FractalbitsNamespace', {
      name: 'fractalbits.local',
      vpc: this.vpc,
    });
    const bssService = privateDnsNamespace.createService('BssService', {
      name: 'bss-server',
      dnsRecordType: servicediscovery.DnsRecordType.A,
      dnsTtl: cdk.Duration.seconds(60),
      routingPolicy: servicediscovery.RoutingPolicy.MULTIVALUE,
    });

    // Security Group
    const sg = new ec2.SecurityGroup(this, 'InstanceSG', {
      vpc: this.vpc,
      securityGroupName: 'FractalbitsInstanceSG',
      description: 'Allow outbound only for SSM and S3 access',
      allowAllOutbound: true,
    });

    let targetIdOutput: cdk.CfnOutput;

    if (props.serviceName == "nss") {
      const bucket = new s3.Bucket(this, 'Bucket', {
        removalPolicy: cdk.RemovalPolicy.DESTROY, // Delete bucket on stack delete
        autoDeleteObjects: true,                  // Empty bucket before deletion
      });
      const nssInstanceType = new ec2.InstanceType(props.nssInstanceType ?? 'm7gd.4xlarge');
      const instance = createInstance(this, this.vpc, `${props.serviceName}_bench`, ec2.SubnetType.PRIVATE_ISOLATED, nssInstanceType, sg, ec2Role);
      const ebsVolume = createEbsVolume(this, 'MultiAttachVolume', az, instance.instanceId);
      const nssBootstrapOptions = `nss_server --bucket=${bucket.bucketName} --volume_id=${ebsVolume.volumeId} --iam_role=${ec2Role.roleName} --meta_stack_testing`;
      instance.addUserData(createUserData(this, nssBootstrapOptions).render());

      targetIdOutput = new cdk.CfnOutput(this, 'instanceId', {
        value: instance.instanceId,
        description: `EC2 instance ID`,
      });

    } else {
      if (!props.bssInstanceTypes) {
        console.error("Error: bssInstanceTypes must be provided for the 'bss' service type in the meta stack. Please specify it via --context bssInstanceTypes='type1,type2'");
        process.exit(1);
      }
      const bssInstanceTypes = props.bssInstanceTypes.split(',');
      const bssBootstrapOptions = `bss_server --service_id=${bssService.serviceId} --meta_stack_testing`;
      const bssAsg = createEc2Asg(
        this,
        'BssAsg',
        this.vpc,
        sg,
        ec2Role,
        bssInstanceTypes,
        bssBootstrapOptions,
        1,
        1,
      );

      new cdk.CustomResource(this, 'DeregisterBssAsgInstances', {
        serviceToken: props.deregisterProviderServiceToken,
        properties: {
          ServiceId: bssService.serviceId,
          NamespaceName: privateDnsNamespace.namespaceName,
          ServiceName: bssService.serviceName,
          AsgName: bssAsg.autoScalingGroupName,
        },
      });

      addAsgDeregistrationLifecycleHook(this, 'Bss', bssAsg, bssService);

      targetIdOutput = new cdk.CfnOutput(this, 'bssAsgName', {
        value: bssAsg.autoScalingGroupName,
        description: `Bss Auto Scaling Group Name`,
      });
    }

    // Output the relevant ID (instance ID or ASG name)
    targetIdOutput;
  }
}
