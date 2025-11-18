import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as elbv2 from "aws-cdk-lib/aws-elasticloadbalancingv2";
import * as elbv2_targets from "aws-cdk-lib/aws-elasticloadbalancingv2-targets";
import * as autoscaling from "aws-cdk-lib/aws-autoscaling";

import {
  createInstance,
  createUserData,
  createEc2Asg,
  createEbsVolume,
  createDynamoDbTable,
  createEc2Role,
  createVpcEndpoints,
  createPrivateLinkNlb,
  addAsgDynamoDbDeregistrationLifecycleHook,
  getAzNameFromIdAtBuildTime,
} from "./ec2-utils";

export interface FractalbitsVpcStackProps extends cdk.StackProps {
  numApiServers: number;
  numBenchClients: number;
  numBssNodes: number;
  benchType?: "service_endpoint" | "external" | null;
  azPair: string;
  bssInstanceTypes: string;
  apiServerInstanceType: string;
  benchClientInstanceType: string;
  browserIp?: string;
  dataBlobStorage: "s3HybridSingleAz" | "s3ExpressMultiAz";
  rootServerHa: boolean;
}

export class FractalbitsVpcStack extends cdk.Stack {
  public readonly nlbLoadBalancerDnsName: string;
  public readonly vpc: ec2.Vpc;

  constructor(scope: Construct, id: string, props: FractalbitsVpcStackProps) {
    super(scope, id, props);
    const forBenchFlag = props.benchType ? " --for_bench" : "";
    const dataBlobStorage = props.dataBlobStorage;

    // === VPC Configuration ===
    const azPair = props.azPair.split(",");
    // Resolve AZ IDs to actual AZ names
    const az1 = getAzNameFromIdAtBuildTime(azPair[0]);
    // Only resolve second AZ for multi-AZ mode
    const az2 =
      dataBlobStorage === "s3ExpressMultiAz"
        ? getAzNameFromIdAtBuildTime(azPair[1])
        : "";

    // Determine availability zones based on storage mode
    const availabilityZones =
      dataBlobStorage === "s3HybridSingleAz"
        ? [az1] // Single AZ for hybrid mode
        : [az1, az2]; // Multi-AZ for express mode

    // Create VPC with specific availability zones using resolved zone names
    this.vpc = new ec2.Vpc(this, "FractalbitsVpc", {
      vpcName: "fractalbits-vpc",
      ipAddresses: ec2.IpAddresses.cidr("10.0.0.0/16"),
      availabilityZones,
      natGateways: 0,
      enableDnsHostnames: true,
      enableDnsSupport: true,
      subnetConfiguration: [
        {
          name: "PrivateSubnet",
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
          cidrMask: 24,
        },
        {
          name: "PublicSubnet",
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
      ],
    });

    const ec2Role = createEc2Role(this);
    createVpcEndpoints(this.vpc);

    const publicSg = new ec2.SecurityGroup(this, "PublicInstanceSG", {
      vpc: this.vpc,
      securityGroupName: "FractalbitsPublicInstanceSG",
      description:
        "Allow inbound on port 80 for public access, and all outbound",
      allowAllOutbound: true,
    });
    publicSg.addIngressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(80),
      "Allow HTTP access from anywhere",
    );

    const privateSg = new ec2.SecurityGroup(this, "PrivateInstanceSG", {
      vpc: this.vpc,
      securityGroupName: "FractalbitsPrivateInstanceSG",
      description:
        "Allow inbound on port 8088 (e.g., from internal sources), and all outbound",
      allowAllOutbound: true,
    });
    privateSg.addIngressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(80),
      "Allow access to port 80 from within VPC",
    );
    privateSg.addIngressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(8088),
      "Allow access to port 8088 from within VPC",
    );
    privateSg.addIngressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(18088),
      "Allow access to port 18088 (management) from within VPC",
    );
    privateSg.addIngressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(9999),
      "Allow access to port 9999 from within VPC",
    );
    if (props.benchType == "external") {
      // Allow incoming traffic on port 7761 for bench clients
      privateSg.addIngressRule(
        ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
        ec2.Port.tcp(7761),
        "Allow access to port 7761 from within VPC",
      );
    }

    const bucket = new s3.Bucket(this, "Bucket", {
      // No bucketName provided – name will be auto-generated
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Delete bucket on stack delete
      autoDeleteObjects: true, // Empty bucket before deletion
    });

    // Create DynamoDB tables
    createDynamoDbTable(
      this,
      "FractalbitsTable",
      "fractalbits-api-keys-and-buckets",
      "id",
    );
    createDynamoDbTable(
      this,
      "ServiceDiscoveryTable",
      "fractalbits-service-discovery",
      "service_id",
    );
    createDynamoDbTable(
      this,
      "LeaderElectionTable",
      "fractalbits-leader-election",
      "key",
    );

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
    const nssInstanceType = new ec2.InstanceType("m7gd.4xlarge");
    const rssInstanceType = new ec2.InstanceType("c7g.medium");
    const benchInstanceType = new ec2.InstanceType("c7g.large");
    const bucketName = bucket.bucketName;

    // Get specific subnets for instances to ensure correct AZ placement
    const privateSubnets = this.vpc.isolatedSubnets;
    const publicSubnets = this.vpc.publicSubnets;
    const subnet1 = privateSubnets[0]; // First AZ (private)
    // Only get second subnet for multi-AZ mode
    const subnet2 =
      dataBlobStorage === "s3ExpressMultiAz" && privateSubnets.length > 1
        ? privateSubnets[1] // Second AZ (private)
        : subnet1; // Use first subnet for single-AZ mode
    const publicSubnet1 = publicSubnets[0]; // First AZ (public)

    const instanceConfigs = [
      {
        id: "rss-A",
        instanceType: rssInstanceType,
        specificSubnet: subnet1,
        sg: privateSg,
      },
      {
        id: "nss-A",
        instanceType: nssInstanceType,
        specificSubnet: subnet1,
        sg: privateSg,
      },
    ];

    // Only create rss-B when rootServerHa is enabled
    if (props.rootServerHa) {
      instanceConfigs.splice(1, 0, {
        id: "rss-B",
        instanceType: rssInstanceType,
        // For s3HybridSingleAz, place rss-B in same AZ as rss-A
        specificSubnet:
          dataBlobStorage === "s3HybridSingleAz" ? subnet1 : subnet2,
        sg: privateSg,
      });
    }

    // Only create nss-B for s3ExpressMultiAz mode
    if (dataBlobStorage === "s3ExpressMultiAz") {
      instanceConfigs.push({
        id: "nss-B",
        instanceType: nssInstanceType,
        specificSubnet: subnet2,
        sg: privateSg,
      });
    }

    if (props.browserIp) {
      const guiServerSg = new ec2.SecurityGroup(this, "GuiServerSG", {
        vpc: this.vpc,
        securityGroupName: "FractalbitsGuiServerSG",
        description: "Allow inbound on port 80 from a specific IP address",
        allowAllOutbound: true,
      });
      guiServerSg.addIngressRule(
        ec2.Peer.ipv4(`${props.browserIp}/32`),
        ec2.Port.tcp(80),
        "Allow access to port 80 from specific IP",
      );
      instanceConfigs.push({
        id: "gui_server",
        instanceType: new ec2.InstanceType("c8g.large"),
        specificSubnet: publicSubnet1,
        sg: guiServerSg,
      });
    }

    let benchClientAsg: autoscaling.AutoScalingGroup | undefined;
    if (props.benchType === "external") {
      // Create bench_server
      instanceConfigs.push({
        id: "bench_server",
        instanceType: benchInstanceType,
        specificSubnet: subnet1,
        sg: privateSg,
      });
      // Create bench_clients in a ASG group
      const benchClientBootstrapOptions = `bench_client`;
      benchClientAsg = createEc2Asg(
        this,
        "benchClientAsg",
        this.vpc,
        subnet1,
        privateSg,
        ec2Role,
        [props.benchClientInstanceType],
        benchClientBootstrapOptions,
        props.numBenchClients,
        props.numBenchClients,
      );
      // Add lifecycle hook for bench_client ASG
      addAsgDynamoDbDeregistrationLifecycleHook(
        this,
        "BenchClient",
        benchClientAsg,
        "bench-client",
        "fractalbits-service-discovery",
      );
    }
    const instances: Record<string, ec2.Instance> = {};
    instanceConfigs.forEach(({ id, instanceType, sg, specificSubnet }) => {
      instances[id] = createInstance(
        this,
        this.vpc,
        id,
        specificSubnet,
        instanceType,
        sg,
        ec2Role,
      );
    });

    // Create bss_server in a ASG group only for hybrid mode
    let bssAsg: autoscaling.AutoScalingGroup | undefined;
    if (dataBlobStorage === "s3HybridSingleAz") {
      const bssBootstrapOptions = `${forBenchFlag} bss_server`;
      bssAsg = createEc2Asg(
        this,
        "BssAsg",
        this.vpc,
        subnet1,
        privateSg,
        ec2Role,
        props.bssInstanceTypes.split(","),
        bssBootstrapOptions,
        props.numBssNodes,
        props.numBssNodes,
      );
      // Add lifecycle hook for bss_server ASG
      addAsgDynamoDbDeregistrationLifecycleHook(
        this,
        "BssServer",
        bssAsg,
        "bss-server",
        "fractalbits-service-discovery",
      );
    }

    // Create PrivateLink setup for NSS and RSS services
    const servicePort = 8088;
    const mirrordPort = 9999;

    // NSS PrivateLink - only for s3ExpressMultiAz mode
    // For single-AZ, use direct instance IP to avoid VPC endpoint latency
    let nssPrivateLink: any;
    if (dataBlobStorage === "s3ExpressMultiAz") {
      const nssTargets = [instances["nss-A"], instances["nss-B"]];
      nssPrivateLink = createPrivateLinkNlb(
        this,
        "Nss",
        this.vpc,
        nssTargets,
        servicePort,
      );
    }

    // NLB for RSS - always create (even in non-HA mode)
    // In HA mode: use both rss-A and rss-B as targets
    // In non-HA mode: use only rss-A as target
    const rssNlb = new elbv2.NetworkLoadBalancer(this, "RssNlb", {
      vpc: this.vpc,
      internetFacing: false,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      crossZoneEnabled: props.rootServerHa,
    });
    const rssListener = rssNlb.addListener("RssListener", {
      port: servicePort,
    });
    const rssTargets = props.rootServerHa
      ? [
          new elbv2_targets.InstanceTarget(instances["rss-A"]),
          new elbv2_targets.InstanceTarget(instances["rss-B"]),
        ]
      : [new elbv2_targets.InstanceTarget(instances["rss-A"])];
    rssListener.addTargets("RssTargets", {
      port: servicePort,
      targets: rssTargets,
      healthCheck: {
        enabled: true,
        healthyThresholdCount: 2,
        unhealthyThresholdCount: 2,
        interval: cdk.Duration.seconds(5),
        timeout: cdk.Duration.seconds(2),
      },
    });

    // Only create mirrord for s3ExpressMultiAz mode
    let mirrordPrivateLink: any;
    if (dataBlobStorage === "s3ExpressMultiAz") {
      mirrordPrivateLink = createPrivateLinkNlb(
        this,
        "Mirrord",
        this.vpc,
        [instances["nss-A"], instances["nss-B"]],
        mirrordPort,
      );
    }

    // Determine NSS endpoint based on mode
    const nssEndpoint =
      dataBlobStorage === "s3ExpressMultiAz"
        ? nssPrivateLink.endpointDns
        : `${instances["nss-A"].instancePrivateIp}`;

    const rssEndpoint = rssNlb.loadBalancerDnsName;

    // Reusable function to create bootstrap options for api_server and gui_server
    const createApiServerBootstrapOptions = (serviceName: string) => {
      if (dataBlobStorage === "s3ExpressMultiAz") {
        return (
          `${forBenchFlag} ${serviceName} ` +
          `--remote_az=${azPair[1]} ` +
          `--nss_endpoint=${nssEndpoint} ` +
          `--rss_endpoint=${rssEndpoint} `
        );
      } else {
        // Single-AZ: use direct NSS IP to avoid VPC endpoint latency
        return (
          `${forBenchFlag} ${serviceName} ` +
          `--bucket=${bucketName} ` +
          `--nss_endpoint=${nssEndpoint} ` +
          `--rss_endpoint=${rssEndpoint} `
        );
      }
    };

    // Create api_server(s) in a ASG group
    const apiServerBootstrapOptions =
      createApiServerBootstrapOptions("api_server");
    const apiServerAsg = createEc2Asg(
      this,
      "ApiServerAsg",
      this.vpc,
      subnet1,
      privateSg,
      ec2Role,
      [props.apiServerInstanceType],
      apiServerBootstrapOptions,
      props.numApiServers,
      props.numApiServers,
    );

    // Add lifecycle hook for api_server ASG
    addAsgDynamoDbDeregistrationLifecycleHook(
      this,
      "ApiServer",
      apiServerAsg,
      "api-server",
      "fractalbits-service-discovery",
    );

    // NLB for API servers - always create regardless of benchType
    const nlb = new elbv2.NetworkLoadBalancer(this, "ApiNLB", {
      vpc: this.vpc,
      internetFacing: false,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      crossZoneEnabled: dataBlobStorage === "s3ExpressMultiAz", // Only enable cross-zone for multi-AZ
    });
    const listener = nlb.addListener("ApiListener", { port: 80 });
    listener.addTargets("ApiTargets", {
      port: 80,
      targets: [apiServerAsg],
      healthCheck: {
        enabled: true,
        healthyThresholdCount: 2,
        unhealthyThresholdCount: 2,
        interval: cdk.Duration.seconds(5),
        timeout: cdk.Duration.seconds(2),
      },
    });

    // Create EBS Volumes for nss_servers using resolved zone names
    const ebsVolumeA = createEbsVolume(
      this,
      "MultiAttachVolumeA",
      subnet1.availabilityZone,
      instances["nss-A"].instanceId,
    );

    // Only create volume B for s3ExpressMultiAz mode
    let ebsVolumeB: any;
    let ebsVolumeBId: string = "";
    if (dataBlobStorage === "s3ExpressMultiAz") {
      ebsVolumeB = createEbsVolume(
        this,
        "MultiAttachVolumeB",
        subnet2.availabilityZone,
        instances["nss-B"].instanceId,
      );
      ebsVolumeBId = ebsVolumeB.volumeId;
    }

    // Create UserData: we need to make it a separate step since we want to get the instance/volume ids
    const nssA = instances["nss-A"].instanceId;
    const nssB = instances["nss-B"]?.instanceId || "";
    const ebsVolumeAId = ebsVolumeA.volumeId;

    // Shared function to create NSS bootstrap options
    const createNssBootstrapOptions = (volumeId: string) => {
      const params = [
        forBenchFlag,
        "nss_server",
        `--bucket=${bucketName}`,
        `--volume_id=${volumeId}`,
        `--iam_role=${ec2Role.roleName}`,
        `--rss_endpoint=${rssEndpoint}`,
      ];
      // Only add mirrord_endpoint for s3ExpressMultiAz mode
      if (dataBlobStorage === "s3ExpressMultiAz" && mirrordPrivateLink) {
        params.push(`--mirrord_endpoint=${mirrordPrivateLink.endpointDns}`);
      }
      return params.filter((p) => p).join(" ");
    };

    const instanceBootstrapOptions = [
      {
        id: "rss-A",
        bootstrapOptions:
          `${forBenchFlag} root_server ` +
          `--nss_endpoint=${nssEndpoint} ` +
          `--nss_a_id=${nssA} ` +
          (nssB ? `--nss_b_id=${nssB} ` : "") +
          `--volume_a_id=${ebsVolumeAId} ` +
          (ebsVolumeBId ? `--volume_b_id=${ebsVolumeBId} ` : "") +
          (props.rootServerHa
            ? `--follower_id=${instances["rss-B"].instanceId} --ha_enabled `
            : "") +
          (dataBlobStorage === "s3ExpressMultiAz"
            ? `--remote_az=${azPair[1]}`
            : `--num_bss_nodes=${props.numBssNodes}`),
      },
      {
        id: "nss-A",
        bootstrapOptions: createNssBootstrapOptions(ebsVolumeAId),
      },
    ];

    // Only add rss-B bootstrap for HA mode
    if (props.rootServerHa) {
      instanceBootstrapOptions.splice(1, 0, {
        id: "rss-B",
        bootstrapOptions:
          `${forBenchFlag} root_server ` +
          `--nss_endpoint=${nssEndpoint} ` +
          `--nss_a_id=${nssA} ` +
          `--volume_a_id=${ebsVolumeAId}`,
      });
    }

    // Only add nss-B bootstrap for s3ExpressMultiAz mode
    if (dataBlobStorage === "s3ExpressMultiAz" && ebsVolumeBId) {
      instanceBootstrapOptions.push({
        id: "nss-B",
        bootstrapOptions: createNssBootstrapOptions(ebsVolumeBId),
      });
    }
    if (props.benchType === "external") {
      instanceBootstrapOptions.push({
        id: "bench_server",
        bootstrapOptions: `bench_server --api_server_endpoint=${nlb.loadBalancerDnsName} --bench_client_num=${props.numBenchClients} `,
      });
    }
    if (props.browserIp) {
      instanceBootstrapOptions.push({
        id: "gui_server",
        bootstrapOptions: createApiServerBootstrapOptions("gui_server"),
      });
    }
    instanceBootstrapOptions.forEach(({ id, bootstrapOptions }) => {
      instances[id]?.addUserData(
        createUserData(this, bootstrapOptions).render(),
      );
    });

    // Outputs
    new cdk.CfnOutput(this, "FractalbitsBucketName", {
      value: bucket.bucketName,
    });

    for (const [id, instance] of Object.entries(instances)) {
      new cdk.CfnOutput(this, `${id} Id`, {
        value: instance.instanceId,
        description: `EC2 instance ${id} ID`,
      });
    }

    new cdk.CfnOutput(this, "ApiNLBDnsName", {
      value: nlb.loadBalancerDnsName,
      description: "DNS name of the API NLB",
    });

    new cdk.CfnOutput(this, "RssEndpointDns", {
      value: rssNlb.loadBalancerDnsName,
      description: "NLB DNS for RSS service",
    });

    // Only output mirrord endpoint for s3ExpressMultiAz mode
    if (dataBlobStorage === "s3ExpressMultiAz" && mirrordPrivateLink) {
      new cdk.CfnOutput(this, "MirrordEndpointDns", {
        value: mirrordPrivateLink.endpointDns,
        description: "VPC Endpoint DNS for Mirrord service",
      });
    }

    this.nlbLoadBalancerDnsName = nlb.loadBalancerDnsName;

    new cdk.CfnOutput(this, "VolumeAId", {
      value: ebsVolumeAId,
      description: "EBS volume A ID",
    });

    // Only output volume B for s3ExpressMultiAz mode
    if (dataBlobStorage === "s3ExpressMultiAz" && ebsVolumeBId) {
      new cdk.CfnOutput(this, "VolumeBId", {
        value: ebsVolumeBId,
        description: "EBS volume B ID",
      });
    }

    if (bssAsg) {
      new cdk.CfnOutput(this, "bssAsgName", {
        value: bssAsg.autoScalingGroupName,
        description: `Bss Auto Scaling Group Name`,
      });
    }

    new cdk.CfnOutput(this, "apiServerAsgName", {
      value: apiServerAsg.autoScalingGroupName,
      description: `Api Server Auto Scaling Group Name`,
    });

    if (benchClientAsg) {
      new cdk.CfnOutput(this, "benchClientAsgName", {
        value: benchClientAsg.autoScalingGroupName,
        description: `Bench Client Auto Scaling Group Name`,
      });
    }

    if (props.browserIp) {
      new cdk.CfnOutput(this, "GuiServerPublicIp", {
        value: instances["gui_server"].instancePublicIp,
        description: "Public IP of the GUI Server",
      });
    }
  }
}
