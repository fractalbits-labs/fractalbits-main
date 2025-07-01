#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { FractalbitsVpcStack } from '../lib/fractalbits-vpc-stack';
import { FractalbitsBenchVpcStack } from '../lib/fractalbits-bench-vpc-stack';
import { PeeringStack } from '../lib/fractalbits-peering-stack';

const app = new cdk.App();

const numApiServers = app.node.tryGetContext('numApiServers') ?? 2;

const vpcStack = new FractalbitsVpcStack(app, 'FractalbitsVpcStack', {
  env: {},
  numApiServers: numApiServers,
});

const benchClientCount = app.node.tryGetContext('benchClientCount') ?? 2;

const benchVpcStack = new FractalbitsBenchVpcStack(app, 'FractalbitsBenchVpcStack', {
  env: {},
  serviceEndpoint: vpcStack.nlbLoadBalancerDnsName,
  benchClientCount: benchClientCount,
});

new PeeringStack(app, 'PeeringStack', {
  vpcA: vpcStack.vpc,
  vpcB: benchVpcStack.vpc,
  env: {},
});

