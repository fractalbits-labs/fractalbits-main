import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as elbv2 from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import * as elbv2_targets from 'aws-cdk-lib/aws-elasticloadbalancingv2-targets';
import * as servicediscovery from 'aws-cdk-lib/aws-servicediscovery';
import { createInstance, createUserData, createEc2Asg, createEbsVolume } from './ec2-utils';

export interface FractalbitsVpcStackProps extends cdk.StackProps {
  numApiServers: number;
  benchType?: "service_endpoint" | "internal" | "external" | null;
  availabilityZone?: string;
}

export class FractalbitsVpcStack extends cdk.Stack {
  public readonly nlbLoadBalancerDnsName: string;
  public readonly vpc: ec2.Vpc;

  constructor(scope: Construct, id: string, props: FractalbitsVpcStackProps) {
    super(scope, id, props);

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
        { name: 'PrivateSubnet', subnetType: ec2.SubnetType.PRIVATE_ISOLATED, cidrMask: 24 },
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

    // Add Gateway Endpoint for S3
    this.vpc.addGatewayEndpoint('S3Endpoint', {
      service: ec2.GatewayVpcEndpointAwsService.S3,
    });

    // Add Gateway Endpoint for DynamoDB
    this.vpc.addGatewayEndpoint('DynamoDbEndpoint', {
      service: ec2.GatewayVpcEndpointAwsService.DYNAMODB,
    });

    // Add Interface Endpoint for EC2, SSM, CloudWatch and CloudMap
    ['SSM', 'SSM_MESSAGES', 'EC2', 'EC2_MESSAGES', 'CLOUDWATCH', 'CLOUDWATCH_LOGS', 'CLOUD_MAP_SERVICE_DISCOVERY'].forEach(service => {
      this.vpc.addInterfaceEndpoint(`${service}Endpoint`, {
        service: (ec2.InterfaceVpcEndpointAwsService as any)[service],
        subnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
        privateDnsEnabled: true,
      });
    });

    // IAM Role for EC2
    const ec2Role = new iam.Role(this, 'InstanceRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMFullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonDynamoDBFullAccess_v2'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonEC2FullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('CloudWatchAgentServerPolicy'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AWSCloudMapFullAccess'),
      ],
    });

    const publicSg = new ec2.SecurityGroup(this, 'PublicInstanceSG', {
      vpc: this.vpc,
      securityGroupName: 'FractalbitsPublicInstanceSG',
      description: 'Allow inbound on port 80 for public access, and outbound for SSM, DDB, S3',
      allowAllOutbound: true,
    });
    publicSg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(80), 'Allow HTTP access from anywhere');
    if (props.benchType === "internal") {
      // Allow incoming traffic on port 7761 for bench clients
      publicSg.addIngressRule(ec2.Peer.ipv4(this.vpc.vpcCidrBlock), ec2.Port.tcp(7761), 'Allow access to port 7761 from within VPC');
    }

    const privateSg = new ec2.SecurityGroup(this, 'PrivateInstanceSG', {
      vpc: this.vpc,
      securityGroupName: 'FractalbitsPrivateInstanceSG',
      description: 'Allow inbound on port 8088 (e.g., from internal sources), and outbound for SSM, DDB, S3',
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
    const rssInstanceType = ec2.InstanceType.of(ec2.InstanceClass.C7G, ec2.InstanceSize.MEDIUM);
    const apiInstanceType = ec2.InstanceType.of(ec2.InstanceClass.C8G, ec2.InstanceSize.LARGE);
    const bucketName = bucket.bucketName;

    const instanceConfigs = [
      { id: 'root_server', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: rssInstanceType, sg: privateSg },
      { id: 'nss_server_primary', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: nssInstanceType, sg: privateSg },
      // { id: 'nss_server_secondary', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: nss_instance_type, sg: privateSg },
    ];

    if (props.benchType === "internal" || props.benchType === "external") {
      // Create bench_server instance
      const benchInstanceType = ec2.InstanceType.of(ec2.InstanceClass.C7G, ec2.InstanceSize.LARGE);
      instanceConfigs.push({ id: 'bench_server', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: benchInstanceType, sg: privateSg });

      if (props.benchType === "external") {
        // Create bench_client instance(s)
        const benchInstanceType = ec2.InstanceType.of(ec2.InstanceClass.C7G, ec2.InstanceSize.LARGE);
        for (let i = 1; i <= props.numApiServers; i++) {
          instanceConfigs.push({ id: `bench_client_${i}`, subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: benchInstanceType, sg: privateSg });
        }
      }
    }

    for (let i = 1; i <= props.numApiServers; i++) {
      instanceConfigs.push({ id: `api_server_${i}`, subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: apiInstanceType, sg: publicSg });
    }

    const instances: Record<string, ec2.Instance> = {};
    instanceConfigs.forEach(({ id, subnet, instanceType, sg}) => {
      instances[id] = createInstance(this, this.vpc, id, subnet, instanceType, sg, ec2Role);
    });

    // Create bss_server separately in a ASG group
    const forBenchFlag = props.benchType ? ' --for_bench' : '';
    const bssBootstrapOptions = `${forBenchFlag} bss_server --bss_service_id=${bssService.serviceId}`;
    let bssAsg = createEc2Asg(
        this,
        'BssAsg',
        this.vpc,
        [az],
        privateSg,
        ec2Role,
        ['i8g.4xlarge', 'i8g.8xlarge', 'i8g.12xlarge', 'i8g.16xlarge', 'i8g.24xlarge', 'i8g.metal-24xl'],
        // ['i3.2xlarge', 'i3en.xlarge', 'i8g.xlarge2', 'is4gn.xlarge'],
        bssBootstrapOptions,
    );

    let nlb: elbv2.NetworkLoadBalancer | undefined;
    if (props.benchType !== "internal" && props.benchType !== "external") {
      // NLB for API servers
      nlb = new elbv2.NetworkLoadBalancer(this, 'ApiNLB', {
        vpc: this.vpc,
        internetFacing: false,
        vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      });

      const listener = nlb.addListener('ApiListener', { port: 80 });

      const apiServerTargets = [];
      for (let i = 1; i <= props.numApiServers; i++) {
        apiServerTargets.push(new elbv2_targets.InstanceTarget(instances[`api_server_${i}`]));
      }

      listener.addTargets('ApiTargets', {
        port: 80,
        targets: apiServerTargets,
      });
    }

    // Create EBS Volume with Multi-Attach for nss_server
    const ebsVolume = createEbsVolume(this, 'MultiAttachVolume', az, instances['nss_server_primary'].instanceId);

    // Create UserData: we need to make it a separate step since we want to get the instance/volume ids
    const primaryNss = instances['nss_server_primary'].instanceId;
    const secondaryNss = instances['nss_server_secondary']?.instanceId ?? null;
    const ebsVolumeId = ebsVolume.volumeId;
    const bssIp = bssService.serviceName + '.' + privateDnsNamespace.namespaceName;
    const nssIp = instances["nss_server_primary"].instancePrivateIp;
    const rssIp = instances["root_server"].instancePrivateIp;

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
    ];

    for (let i = 1; i <= props.numApiServers; i++) {
      let apiBootstrapOptions = `${forBenchFlag} api_server --bucket=${bucketName} --bss_ip=${bssIp} --nss_ip=${nssIp} --rss_ip=${rssIp}`;
      if (props.benchType === "internal") {
        apiBootstrapOptions += ` --with_bench_client`;
      }
      instanceBootstrapOptions.push({
        id: `api_server_${i}`,
        bootstrapOptions: apiBootstrapOptions
      });
    }

    if (props.benchType === "internal" || props.benchType === "external") {
      const apiServerPrivateIps: string[] = [];
      for (let i = 1; i <= props.numApiServers; i++) {
        apiServerPrivateIps.push(instances[`api_server_${i}`].instancePrivateIp);
      }

      let benchServerBootstrapOptions: string;
      if (props.benchType === "external") {
        const benchClientPrivateIps: string[] = [];
        for (let i = 1; i <= props.numApiServers; i++) {
          benchClientPrivateIps.push(instances[`bench_client_${i}`].instancePrivateIp);
        }
        benchServerBootstrapOptions = `bench_server --client_ips=${benchClientPrivateIps.join(',')} --api_server_ips=${apiServerPrivateIps.join(',')}`;

        for (let i = 1; i <= props.numApiServers; i++) {
          const apiServerPrivateIp = apiServerPrivateIps[i - 1];
          const benchClientBootstrapOptions = `bench_client --api_server_pair_ip=${apiServerPrivateIp}`;
          instanceBootstrapOptions.push({
            id: `bench_client_${i}`,
            bootstrapOptions: benchClientBootstrapOptions
          });
        }
      } else { // internal
        let ips = apiServerPrivateIps.join(',');
        benchServerBootstrapOptions = `bench_server --client_ips=${ips} --api_server_ips=${ips}`;
      }

      instanceBootstrapOptions.push({
        id: 'bench_server',
        bootstrapOptions: benchServerBootstrapOptions
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

    if (props.benchType === "internal") {
      new cdk.CfnOutput(this, 'BenchServerId', {
        value: instances['bench_server'].instanceId,
        description: 'EC2 instance bench_server ID',
      });
    } else if (props.benchType === "external") {
      for (let i = 1; i <= props.numApiServers; i++) {
        new cdk.CfnOutput(this, `BenchClient_${i}_Id`, {
          value: instances[`bench_client_${i}`].instanceId,
          description: `EC2 instance bench_client_${i} ID`,
        });
      }
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

    new cdk.CfnOutput(this, 'AsgName', {
      value: bssAsg.autoScalingGroupName,
      description: `Bss Auto Scaling Group Name`,
    });
  }
}
