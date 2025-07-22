import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as elbv2 from 'aws-cdk-lib/aws-elasticloadbalancingv2';
// import * as elbv2_targets from 'aws-cdk-lib/aws-elasticloadbalancingv2-targets';
import * as servicediscovery from 'aws-cdk-lib/aws-servicediscovery';

import { createInstance, createUserData, createEc2Asg, createEbsVolume, createDeregisterLambda, createDeregisterProvider, createDeregisterInstanceCustomResource } from './ec2-utils';

export interface FractalbitsVpcStackProps extends cdk.StackProps {
  numApiServers: number;
  numBenchClients: number;
  benchType?: "service_endpoint" | "external" | null;
  availabilityZone?: string;
  bssInstanceTypes: string;
}

export class FractalbitsVpcStack extends cdk.Stack {
  public readonly nlbLoadBalancerDnsName: string;
  public readonly vpc: ec2.Vpc;

  constructor(scope: Construct, id: string, props: FractalbitsVpcStackProps) {
    super(scope, id, props);
    const forBenchFlag = props.benchType ? ' --for_bench' : '';

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

    // IAM Role for Lambda Deregistration Function
    const deregisterLambdaRole = new iam.Role(this, 'DeregisterLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
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
    const apiServerService = privateDnsNamespace.createService('ApiServerService', {
        name: 'api-server',
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
    const bucketName = bucket.bucketName;

    const instanceConfigs = [
      { id: 'root_server', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: rssInstanceType, sg: privateSg },
      { id: 'nss_server_primary', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: nssInstanceType, sg: privateSg },
      // { id: 'nss_server_secondary', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: nss_instance_type, sg: privateSg },
    ];

    if (props.benchType === "external") {
      const benchInstanceType = ec2.InstanceType.of(ec2.InstanceClass.C7G, ec2.InstanceSize.LARGE);
      // Create bench_server instance
      instanceConfigs.push({ id: 'bench_server', subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: benchInstanceType, sg: privateSg });
      // Create bench_client instance(s)
      for (let i = 1; i <= props.numApiServers; i++) {
        instanceConfigs.push({ id: `bench_client_${i}`, subnet: ec2.SubnetType.PRIVATE_ISOLATED, instanceType: benchInstanceType, sg: privateSg });
      }
    }

    const instances: Record<string, ec2.Instance> = {};
    instanceConfigs.forEach(({ id, subnet, instanceType, sg}) => {
      instances[id] = createInstance(this, this.vpc, id, subnet, instanceType, sg, ec2Role);
    });

    // Create bss_server in a ASG group
    const bssBootstrapOptions = `${forBenchFlag} bss_server --bss_service_id=${bssService.serviceId}`;
    const bssAsg = createEc2Asg(
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

    // Create api_server(s) in a ASG group
    const apiServerBootstrapOptions = `${forBenchFlag} api_server --bucket=${bucket.bucketName} --bss_ip=${bssService.serviceName}.${privateDnsNamespace.namespaceName} --nss_ip=${instances["nss_server_primary"].instancePrivateIp} --rss_ip=${instances["root_server"].instancePrivateIp} --api_server_service_id=${apiServerService.serviceId}`;
    const apiServerAsg = createEc2Asg(
        this,
        'ApiServerAsg',
        this.vpc,
        publicSg,
        ec2Role,
        ['c8g.large'],
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
        vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      });

      const listener = nlb.addListener('ApiListener', { port: 80 });

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
    ];

    if (props.benchType === "external") {
      const benchClientPrivateIps: string[] = [];
      for (let i = 1; i <= props.numBenchClients; i++) {
        benchClientPrivateIps.push(instances[`bench_client_${i}`].instancePrivateIp);
        instanceBootstrapOptions.push({
          id: `bench_client_${i}`,
          bootstrapOptions: `bench_client`,
        });
      }
      instanceBootstrapOptions.push({
        id: 'bench_server',
        bootstrapOptions: `bench_server --client_ips=${benchClientPrivateIps.join(',')}`,
      });
    }

    instanceBootstrapOptions.forEach(({id, bootstrapOptions}) => {
      instances[id]?.addUserData(createUserData(this, bootstrapOptions).render())
    })

    const deregisterLambda = createDeregisterLambda(this, deregisterLambdaRole);
    const deregisterProvider = createDeregisterProvider(this, deregisterLambda);

    createDeregisterInstanceCustomResource(this, 'DeregisterBssAsgInstances', deregisterProvider, bssService, privateDnsNamespace, bssAsg);
    createDeregisterInstanceCustomResource(this, 'DeregisterApiServerAsgInstances', deregisterProvider, apiServerService, privateDnsNamespace, apiServerAsg);

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

    if (props.benchType === "external") {
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

    new cdk.CfnOutput(this, 'bssAsgName', {
      value: bssAsg.autoScalingGroupName,
      description: `Bss Auto Scaling Group Name`,
    });

    new cdk.CfnOutput(this, 'apiServerAsgName', {
      value: apiServerAsg.autoScalingGroupName,
      description: `Api Server Auto Scaling Group Name`,
    });
  }
}
