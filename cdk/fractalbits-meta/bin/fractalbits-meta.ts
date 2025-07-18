#!/home/linuxbrew/.linuxbrew/opt/node/bin/node
import * as cdk from 'aws-cdk-lib';
import { FractalbitsMetaStack } from '../lib/fractalbits-meta-stack';

const app = new cdk.App();
const bssUseI3 = app.node.tryGetContext('bssUseI3') ?? false;

new FractalbitsMetaStack(app, 'FractalbitsMetaStack-Nss', {
  serviceName: 'nss',
  bssUseI3: bssUseI3,
});

new FractalbitsMetaStack(app, 'FractalbitsMetaStack-Bss', {
  serviceName: 'bss',
  bssUseI3: bssUseI3,
});
