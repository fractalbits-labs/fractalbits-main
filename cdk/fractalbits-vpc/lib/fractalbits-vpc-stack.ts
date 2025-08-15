import * as cdk from 'aws-cdk-lib';
import {Construct} from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3express from 'aws-cdk-lib/aws-s3express';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as elbv2 from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import * as autoscaling from 'aws-cdk-lib/aws-autoscaling';

import {createInstance, createUserData, createEc2Asg, createEbsVolume, createServiceDiscoveryTable, createEc2Role, createVpcEndpoints} from './ec2-utils';

export interface FractalbitsVpcStackProps extends cdk.StackProps {
  numApiServers: number;
  numBenchClients: number;
  benchType?: "service_endpoint" | "external" | null;
  availabilityZone?: string;
  bssInstanceTypes: string;
  browserIp?: string;
  dataBlobStorage?: "hybrid" | "s3Express";
}

export class FractalbitsVpcStack extends cdk.Stack {
  public readonly nlbLoadBalancerDnsName: string;
  public readonly vpc: ec2.Vpc;

  constructor(scope: Construct, id: string, props: FractalbitsVpcStackProps) {
    super(scope, id, props);
    const forBenchFlag = props.benchType ? ' --for_bench' : '';
    const dataBlobStorage = props.dataBlobStorage ?? 's3Express';

    // === VPC Configuration ===
    const az = props.availabilityZone ?? this.availabilityZones[this.availabilityZones.length - 1];
    this.vpc = new ec2.Vpc(this, 'FractalbitsVpc', {
      vpcName: 'fractalbits-vpc',
      ipAddresses: ec2.IpAddresses.cidr('10.0.0.0/16'),
      availabilityZones: [az],
      natGateways: 0,
      enableDnsHostnames: true,
      enableDnsSupport: true,
      subnetConfiguration: [
        {name: 'PrivateSubnet', subnetType: ec2.SubnetType.PRIVATE_ISOLATED, cidrMask: 24},
        {name: 'PublicSubnet', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24},
      ],
    });

    const ec2Role = createEc2Role(this);
    createVpcEndpoints(this.vpc);

    const publicSg = new ec2.SecurityGroup(this, 'PublicInstanceSG', {
      vpc: this.vpc,
      securityGroupName: 'FractalbitsPublicInstanceSG',
      description: 'Allow inbound on port 80 for public access, and all outbound',
      allowAllOutbound: true,
    });
    publicSg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(80), 'Allow HTTP access from anywhere');

    const privateSg = new ec2.SecurityGroup(this, 'PrivateInstanceSG', {
      vpc: this.vpc,
      securityGroupName: 'FractalbitsPrivateInstanceSG',
      description: 'Allow inbound on port 8088 (e.g., from internal sources), and all outbound',
      allowAllOutbound: true,
    });
    privateSg.addIngressRule(ec2.Peer.ipv4(this.vpc.vpcCidrBlock), ec2.Port.tcp(8088), 'Allow access to port 8088 from within VPC');
    if (props.benchType == "external") {
      // Allow incoming traffic on port 7761 for bench clients
      privateSg.addIngressRule(ec2.Peer.ipv4(this.vpc.vpcCidrBlock), ec2.Port.tcp(7761), 'Allow access to port 7761 from within VPC');
    }

    const bucket = new s3.Bucket(this, 'Bucket', {
      // No bucketName provided – name will be auto-generated
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Delete bucket on stack delete
      autoDeleteObjects: true,                  // Empty bucket before deletion
    });

    // Create S3 Express One Zone bucket for high-performance blob storage when in s3Express mode
    let dataBlobBucket: s3express.CfnDirectoryBucket | undefined;
    if (dataBlobStorage === 's3Express') {
      // For S3 Express, derive zone ID from availability zone
      // Map us-west-2[a-d] to usw2-az[1-4] using CloudFormation mappings
      const azMappings = new cdk.CfnMapping(this, 'AZToZoneIdMapping', {
        mapping: {
          'us-west-2a': {zoneId: 'usw2-az2'},
          'us-west-2b': {zoneId: 'usw2-az1'},
          'us-west-2c': {zoneId: 'usw2-az3'},
          'us-west-2d': {zoneId: 'usw2-az4'},
        },
        lazy: true,
      });

      const zoneId = azMappings.findInMap(az, 'zoneId');
      const bucketName = cdk.Fn.sub('fractalbits-express--${ZoneId}--x-s3', {
        ZoneId: zoneId
      });

      dataBlobBucket = new s3express.CfnDirectoryBucket(this, 'DataBlobExpressBucket', {
        bucketName: bucketName,
        dataRedundancy: 'SingleAvailabilityZone',
        locationName: zoneId,
        // Note: CfnDirectoryBucket doesn't support removalPolicy/autoDeleteObjects like regular buckets
      });
    }

    new dynamodb.Table(this, 'FractalbitsTable', {
      partitionKey: {
        name: 'id',
        type: dynamodb.AttributeType.STRING,
      },
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Delete table on stack delete
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      tableName: 'fractalbits-keys-and-buckets',
    });

    createServiceDiscoveryTable(this);

    new dynamodb.Table(this, 'LeaderElectionTable', {
      partitionKey: {
        name: 'key',
        type: dynamodb.AttributeType.STRING,
      },
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Delete table on stack delete
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      tableName: 'fractalbits-leader-election',
    });

    // new dynamodb.Table(this, 'EBSFailoverStateTable', {
    //   partitionKey: {
    //     name: 'VolumeId',
    //     type: dynamodb.AttributeType.STRING,
    //   },
    //   removalPolicy: cdk.RemovalPolicy.DESTROY, // Delete table on stack delete
    //   billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
    //   tableName: 'ebs-failover-state',
    // });

    // Define instance metadata, and create instances
    const nssInstanceType = new ec2.InstanceType('m7gd.4xlarge');
    const rssInstanceType = new ec2.InstanceType('c7g.medium');
    const benchInstanceType = new ec2.InstanceType('c7g.large');
    const bucketName = bucket.bucketName;
    const instanceConfigs = [
      {id: 'root_server', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: rssInstanceType, sg: privateSg},
      {id: 'nss_server_primary', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: nssInstanceType, sg: privateSg},
      // { id: 'nss_server_secondary', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: nss_instance_type, sg: privateSg },
    ];

    if (props.browserIp) {
      const guiServerSg = new ec2.SecurityGroup(this, 'GuiServerSG', {
        vpc: this.vpc,
        securityGroupName: 'FractalbitsGuiServerSG',
        description: 'Allow inbound on port 80 from a specific IP address',
        allowAllOutbound: true,
      });
      guiServerSg.addIngressRule(ec2.Peer.ipv4(`${props.browserIp}/32`), ec2.Port.tcp(80), 'Allow access to port 80 from specific IP');
      instanceConfigs.push({id: 'gui_server', subnet: ec2.SubnetType.PUBLIC, instanceType: new ec2.InstanceType('c8g.large'), sg: guiServerSg});
    }

    let benchClientAsg: autoscaling.AutoScalingGroup | undefined;
    if (props.benchType === "external") {
      // Create bench_server
      instanceConfigs.push({id: 'bench_server', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: benchInstanceType, sg: privateSg});
      // Create bench_clients in a ASG group
      const benchClientBootstrapOptions = `bench_client`;
      benchClientAsg = createEc2Asg(
        this,
        'benchClientAsg',
        this.vpc,
        privateSg,
        ec2Role,
        ['c8g.xlarge'],
        benchClientBootstrapOptions,
        props.numBenchClients,
        props.numBenchClients
      );
    }
    const instances: Record<string, ec2.Instance> = {};
    instanceConfigs.forEach(({id, subnet, instanceType, sg}) => {
      instances[id] = createInstance(this, this.vpc, id, subnet, instanceType, sg, ec2Role);
    });

    // Create bss_server in a ASG group only for hybrid mode
    let bssAsg: autoscaling.AutoScalingGroup | undefined;
    if (dataBlobStorage === 'hybrid') {
      const bssBootstrapOptions = `${forBenchFlag} bss_server`;
      bssAsg = createEc2Asg(
        this,
        'BssAsg',
        this.vpc,
        privateSg,
        ec2Role,
        props.bssInstanceTypes.split(','),
        bssBootstrapOptions,
        1,
        1
      );
    }

    // Create api_server(s) in a ASG group
    const dataBlobBucketName = dataBlobStorage === 's3Express' ? dataBlobBucket!.bucketName! : bucket.bucketName;
    const apiServerBootstrapOptions = `${forBenchFlag} api_server --bucket=${dataBlobBucketName} --nss_ip=${instances["nss_server_primary"].instancePrivateIp} --rss_ip=${instances["root_server"].instancePrivateIp}`;
    const apiServerAsg = createEc2Asg(
      this,
      'ApiServerAsg',
      this.vpc,
      publicSg,
      ec2Role,
      ['c8g.xlarge'],
      apiServerBootstrapOptions,
      props.numApiServers,
      props.numApiServers
    );

    let nlb: elbv2.NetworkLoadBalancer | undefined;
    if (props.benchType !== "external") {
      // NLB for API servers
      nlb = new elbv2.NetworkLoadBalancer(this, 'ApiNLB', {
        vpc: this.vpc,
        internetFacing: false,
        vpcSubnets: {subnetType: ec2.SubnetType.PRIVATE_ISOLATED},
      });

      const listener = nlb.addListener('ApiListener', {port: 80});

      listener.addTargets('ApiTargets', {
        port: 80,
        targets: [apiServerAsg],
      });
    }

    // Create EBS Volume with Multi-Attach for nss_server
    const ebsVolume = createEbsVolume(this, 'MultiAttachVolume', az, instances['nss_server_primary'].instanceId);

    // Create UserData: we need to make it a separate step since we want to get the instance/volume ids
    const primaryNss = instances['nss_server_primary'].instanceId;
    const secondaryNss = instances['nss_server_secondary']?.instanceId ?? null;
    const ebsVolumeId = ebsVolume.volumeId;
    const instanceBootstrapOptions = [
      {
        id: 'root_server',
        bootstrapOptions: `${forBenchFlag} root_server --primary_instance_id=${primaryNss} --secondary_instance_id=${secondaryNss} --volume_id=${ebsVolumeId}`
      },
      {
        id: 'nss_server_primary',
        bootstrapOptions: `${forBenchFlag} nss_server --bucket=${bucketName} --volume_id=${ebsVolumeId} --iam_role=${ec2Role.roleName}`
      },
      {
        id: 'nss_server_secondary',
        bootstrapOptions: `${forBenchFlag} nss_server --bucket=${bucketName} --volume_id=${ebsVolumeId} --iam_role=${ec2Role.roleName}`
      },
      {
        id: 'gui_server',
        bootstrapOptions: `gui_server --bucket=${bucket.bucketName} --nss_ip=${instances["nss_server_primary"].instancePrivateIp} --rss_ip=${instances["root_server"].instancePrivateIp}`
      },
    ];
    if (props.benchType === "external") {
      instanceBootstrapOptions.push({
        id: 'bench_server',
        bootstrapOptions: `bench_server --api_server_num=${props.numApiServers} --bench_client_num=${props.numBenchClients}`,
      });
    }
    if (props.browserIp) {
      instanceBootstrapOptions.push({
        id: 'gui_server',
        bootstrapOptions: `${forBenchFlag} gui_server --bucket=${bucket.bucketName} --nss_ip=${instances["nss_server_primary"].instancePrivateIp} --rss_ip=${instances["root_server"].instancePrivateIp}`
      });
    }
    instanceBootstrapOptions.forEach(({id, bootstrapOptions}) => {
      instances[id]?.addUserData(createUserData(this, bootstrapOptions).render())
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
      value: nlb ? nlb.loadBalancerDnsName : 'NLB not created',
      description: 'DNS name of the API NLB',
    });

    this.nlbLoadBalancerDnsName = nlb ? nlb.loadBalancerDnsName : "";

    new cdk.CfnOutput(this, 'VolumeId', {
      value: ebsVolumeId,
      description: 'EBS volume ID',
    });

    if (bssAsg) {
      new cdk.CfnOutput(this, 'bssAsgName', {
        value: bssAsg.autoScalingGroupName,
        description: `Bss Auto Scaling Group Name`,
      });
    }

    if (dataBlobBucket) {
      new cdk.CfnOutput(this, 'DataBlobExpressBucketName', {
        value: dataBlobBucket.bucketName!,
        description: 'S3 Express One Zone bucket for data blobs',
      });
    }

    new cdk.CfnOutput(this, 'apiServerAsgName', {
      value: apiServerAsg.autoScalingGroupName,
      description: `Api Server Auto Scaling Group Name`,
    });

    if (benchClientAsg) {
      new cdk.CfnOutput(this, 'benchClientAsgName', {
        value: benchClientAsg.autoScalingGroupName,
        description: `Bench Client Auto Scaling Group Name`,
      });
    }

    if (props.browserIp) {
      new cdk.CfnOutput(this, 'GuiServerPublicIp', {
        value: instances['gui_server'].instancePublicIp,
        description: 'Public IP of the GUI Server',
      });
    }
  }
}
