#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { FractalbitsVpcStack } from '../lib/fractalbits-vpc-stack';
import { FractalbitsBenchVpcStack } from '../lib/fractalbits-bench-vpc-stack';
import { PeeringStack } from '../lib/fractalbits-peering-stack';
import { FractalbitsMetaStack } from '../lib/fractalbits-meta-stack';

const app = new cdk.App();

const numApiServers = app.node.tryGetContext('numApiServers') ?? 1;
const benchType = app.node.tryGetContext('benchType') ?? null;
const availabilityZone = app.node.tryGetContext('availabilityZone') ?? app.node.tryGetContext('az') ?? undefined;

const vpcStack = new FractalbitsVpcStack(app, 'FractalbitsVpcStack', {
  env: {},
  numApiServers: numApiServers,
  benchType: benchType,
  availabilityZone: availabilityZone,
});

if (benchType === "service_endpoint") {
  const benchClientCount = app.node.tryGetContext('benchClientCount') ?? 1;

  const benchVpcStack = new FractalbitsBenchVpcStack(app, 'FractalbitsBenchVpcStack', {
    env: {},
    serviceEndpoint: vpcStack.nlbLoadBalancerDnsName,
    benchClientCount: benchClientCount,
    benchType: benchType,
  });

  new PeeringStack(app, 'PeeringStack', {
    vpcA: vpcStack.vpc,
    vpcB: benchVpcStack.vpc,
    env: {},
  });
}

// === meta stack ===
const nssInstanceType = app.node.tryGetContext('nssInstanceType') ?? null;
const bssInstanceType = app.node.tryGetContext('bssInstanceType') ?? null;
new FractalbitsMetaStack(app, 'FractalbitsMetaStack-Nss', {
  serviceName: 'nss',
  nssInstanceType: nssInstanceType,
  availabilityZone: availabilityZone,
});

new FractalbitsMetaStack(app, 'FractalbitsMetaStack-Bss', {
  serviceName: 'bss',
  bssInstanceType: bssInstanceType,
  availabilityZone: availabilityZone,
});
