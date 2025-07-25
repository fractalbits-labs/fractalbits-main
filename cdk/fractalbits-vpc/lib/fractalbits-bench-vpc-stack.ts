import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as servicediscovery from 'aws-cdk-lib/aws-servicediscovery';
import { createInstance, createUserData, createEc2Asg} from './ec2-utils';
import * as custom_resources from 'aws-cdk-lib/custom-resources';
import { FractalbitsHelperStack } from './fractalbits-helper-stack';

interface FractalbitsBenchVpcStackProps extends cdk.StackProps {
  serviceEndpoint: string;
  benchClientCount: number;
  benchType?: "service_endpoint" | "internal" | "external" | null;
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
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonDynamoDBFullAccess_v2'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonEC2FullAccess'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AWSCloudMapFullAccess'),
      ],
    });

    const privateDnsNamespace = new servicediscovery.PrivateDnsNamespace(this, 'FractalbitsBenchNamespace', {
        name: 'fractalbits-bench.local',
        vpc: this.vpc,
    });

    const benchClientService = privateDnsNamespace.createService('BenchClientService', {
        name: 'bench-client',
        dnsRecordType: servicediscovery.DnsRecordType.A,
        dnsTtl: cdk.Duration.seconds(60),
        routingPolicy: servicediscovery.RoutingPolicy.MULTIVALUE,
    });

    const privateSg = new ec2.SecurityGroup(this, 'BenchPrivateInstanceSG', {
      vpc: this.vpc,
      securityGroupName: 'FractalbitsBenchPrivateInstanceSG',
      description: 'Allow outbound for SSM',
      allowAllOutbound: true,
    });

    // Allow incoming traffic on port 7761 for bench clients
    privateSg.addIngressRule(ec2.Peer.ipv4(this.vpc.vpcCidrBlock), ec2.Port.tcp(7761), 'Allow incoming on port 7761 from VPC');

    // Bench Server Instance
    const benchServerInstance = createInstance(this, this.vpc, 'BenchServerInstance', ec2.SubnetType.PRIVATE_ISOLATED, ec2.InstanceType.of(ec2.InstanceClass.C7G, ec2.InstanceSize.MEDIUM), privateSg, ec2Role);

    // Bench Client ASG
    const benchClientBootstrapOptions = `bench_client --service_id=${benchClientService.serviceId}`;
    const benchClientAsg = createEc2Asg(
        this,
        'BenchClientAsg',
        this.vpc,
        privateSg,
        ec2Role,
        ['c7g.medium'],
        benchClientBootstrapOptions,
        props.benchClientCount,
        props.benchClientCount
    );

    const bootstrapOptions = `bench_server --api_server_service_endpoint=${props.serviceEndpoint} --bench_client_service_id=${benchClientService.serviceId} --bench_client_num=${props.benchClientCount}`;
    benchServerInstance.addUserData(createUserData(this, bootstrapOptions).render());

    // Outputs
    new cdk.CfnOutput(this, 'BenchServerInstanceId', {
      value: benchServerInstance.instanceId,
      description: 'EC2 instance ID for the bench server',
    });

    new cdk.CfnOutput(this, 'BenchClientAsgName', {
      value: benchClientAsg.autoScalingGroupName,
      description: 'Auto Scaling Group Name for bench clients',
    });

    const helperStack = new FractalbitsHelperStack(this, 'FractalbitsHelperStack');

    new cdk.CustomResource(this, 'DeregisterBenchClientAsgInstances', {
      serviceToken: helperStack.deregisterProviderServiceToken,
      properties: {
        ServiceId: benchClientService.serviceId,
        NamespaceName: privateDnsNamespace.namespaceName,
        ServiceName: benchClientService.serviceName,
        AsgName: benchClientAsg.autoScalingGroupName,
      },
    });
  }
}
