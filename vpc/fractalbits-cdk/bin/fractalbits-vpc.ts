#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { FractalbitsVpcStack } from "../lib/fractalbits-vpc-stack";
import { FractalbitsBenchVpcStack } from "../lib/fractalbits-bench-vpc-stack";
import { PeeringStack } from "../lib/fractalbits-peering-stack";
import { FractalbitsMetaStack } from "../lib/fractalbits-meta-stack";
import { VpcWithPrivateLinkStack } from "../lib/vpc-with-private-link-stack";
import { S3ExpressCrossAzTestStack } from "../lib/s3express-cross-az-test-stack";

const app = new cdk.App();

const numApiServers = app.node.tryGetContext("numApiServers") ?? 1;
const numBenchClients = app.node.tryGetContext("numBenchClients") ?? 1;
const numBssNodes = app.node.tryGetContext("numBssNodes") ?? 6;
const benchType = app.node.tryGetContext("benchType") ?? null;
const bssInstanceTypes =
  app.node.tryGetContext("bssInstanceTypes") ?? "i8g.2xlarge";
const browserIp = app.node.tryGetContext("browserIp") ?? null;
const dataBlobStorage =
  app.node.tryGetContext("dataBlobStorage") ?? "s3HybridSingleAz";

// Get the current region - CDK will auto-detect from AWS config/credentials
const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

// Set default azPair based on region
let defaultAzPair: string;
if (env.region === "us-east-1") {
  defaultAzPair = "use1-az4,use1-az6";
} else {
  defaultAzPair = "usw2-az3,usw2-az4";
}
const azPair = app.node.tryGetContext("azPair") ?? defaultAzPair;

const vpcStack = new FractalbitsVpcStack(app, "FractalbitsVpcStack", {
  env: env,
  browserIp: browserIp,
  numApiServers: numApiServers,
  numBenchClients: numBenchClients,
  numBssNodes: numBssNodes,
  benchType: benchType,
  azPair: azPair,
  bssInstanceTypes: bssInstanceTypes,
  dataBlobStorage: dataBlobStorage,
});

if (benchType === "service_endpoint") {
  const benchClientCount = app.node.tryGetContext("benchClientCount") ?? 1;

  const benchVpcStack = new FractalbitsBenchVpcStack(
    app,
    "FractalbitsBenchVpcStack",
    {
      env: env,
      serviceEndpoint: vpcStack.nlbLoadBalancerDnsName,
      benchClientCount: benchClientCount,
      benchType: benchType,
    },
  );

  new PeeringStack(app, "PeeringStack", {
    vpcA: vpcStack.vpc,
    vpcB: benchVpcStack.vpc,
    env: env,
  });
}

// === meta stack ===
const nssInstanceType = app.node.tryGetContext("nssInstanceType") ?? null;
new FractalbitsMetaStack(app, "FractalbitsMetaStack-Nss", {
  serviceName: "nss",
  nssInstanceType: nssInstanceType,
});

new FractalbitsMetaStack(app, "FractalbitsMetaStack-Bss", {
  serviceName: "bss",
  bssInstanceTypes: bssInstanceTypes,
});

// === VpcWithPrivateLinkStack ===
new VpcWithPrivateLinkStack(app, "VpcWithPrivateLinkStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: "us-east-2",
  },
});

// === S3ExpressCrossAzTestStack ===
new S3ExpressCrossAzTestStack(app, "S3ExpressCrossAzTestStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: "us-east-2",
  },
  targetAz: "use2-az1",
});
