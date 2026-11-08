import { App } from 'aws-cdk-lib';
import { SkyBookStack } from '../lib/skybook-stack';

// `npm run synth` writes CloudFormation to cdk.out/ - nothing is deployed and no AWS account is needed.
const app = new App({ outdir: process.env.CDK_OUTDIR ?? 'cdk.out' });
new SkyBookStack(app, 'SkyBook', {
  env: { account: process.env.CDK_DEFAULT_ACCOUNT ?? '111111111111', region: process.env.CDK_DEFAULT_REGION ?? 'us-east-2' },
  description: 'SkyBook flight booking - production blueprint (MSK, RDS, ElastiCache, EKS, SQS, WAF)',
});
app.synth();
