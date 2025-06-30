#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { FractalbitsVpcStack } from '../lib/fractalbits-vpc-stack';
import { FractalbitsBenchVpcStack } from '../lib/fractalbits-bench-vpc-stack';
import { PeeringStack } from '../lib/fractalbits-peering-stack';

const app = new cdk.App();

const vpcStack = new FractalbitsVpcStack(app, 'FractalbitsVpcStack', {
  env: {},
});

const benchVpcStack = new FractalbitsBenchVpcStack(app, 'FractalbitsBenchVpcStack', {
  env: {},
  serviceEndpoint: vpcStack.nlbLoadBalancerDnsName,
});

new PeeringStack(app, 'PeeringStack', {
  vpcA: vpcStack.vpc,
  vpcB: benchVpcStack.vpc,
  env: {},
});

