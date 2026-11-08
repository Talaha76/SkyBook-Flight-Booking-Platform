/**
 * Synthesizes the stack and asserts the blueprint's key properties with aws-cdk-lib/assertions.
 * Run: npm test   (no AWS account needed)
 */
import assert from 'assert';
import { App } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { SERVICE_DATABASES, SkyBookStack } from '../lib/skybook-stack';

const app = new App();
const stack = new SkyBookStack(app, 'Check', { env: { account: '111111111111', region: 'ca-central-1' } });
const t = Template.fromStack(stack);

const checks: [string, () => void][] = [
  ['one RDS PostgreSQL instance per service, encrypted', () => {
    t.resourceCountIs('AWS::RDS::DBInstance', SERVICE_DATABASES.length);
    t.allResourcesProperties('AWS::RDS::DBInstance', { Engine: 'postgres', StorageEncrypted: true });
  }],
  ['MSK Serverless with IAM auth', () => t.hasResourceProperties('AWS::MSK::ServerlessCluster', { ClientAuthentication: { Sasl: { Iam: { Enabled: true } } } })],
  ['ElastiCache Redis Multi-AZ, encrypted in transit', () => t.hasResourceProperties('AWS::ElastiCache::ReplicationGroup', { Engine: 'redis', MultiAZEnabled: true, TransitEncryptionEnabled: true })],
  ['EKS cluster + managed node group', () => { t.resourceCountIs('AWS::EKS::Cluster', 1); t.resourceCountIs('AWS::EKS::Nodegroup', 1); }],
  ['SQS flight-events queue with DLQ', () => t.hasResourceProperties('AWS::SQS::Queue', { QueueName: 'skybook-flight-events', RedrivePolicy: Match.objectLike({ maxReceiveCount: 5 }) })],
  ['bridge role: SQS permission is exactly sqs:SendMessage on one queue (least privilege)', () => {
    const policies = t.findResources('AWS::IAM::Policy');
    const bridge = Object.entries(policies).find(([id]) => id.startsWith('BridgeRole'));
    assert(bridge, 'bridge policy missing');
    const statements: any[] = (bridge[1] as any).Properties.PolicyDocument.Statement;
    const sqsStatements = statements.filter((s) => [].concat(s.Action).some((a: string) => a.startsWith('sqs:')));
    assert.strictEqual(sqsStatements.length, 1);
    assert.deepStrictEqual(sqsStatements[0].Action, 'sqs:SendMessage');
    assert(!JSON.stringify(sqsStatements[0].Resource).includes('*'), 'SQS resource must not be a wildcard');
  }],
  ['bridge role is assumable only via IRSA web identity', () => t.hasResourceProperties('AWS::IAM::Role', {
    RoleName: 'skybook-aws-bridge',
    AssumeRolePolicyDocument: Match.objectLike({ Statement: [Match.objectLike({ Action: 'sts:AssumeRoleWithWebIdentity' })] }),
  })],
  ['IRSA trust condition keys use the issuer host without https://', () => {
    const c = Object.entries(t.findResources('AWS::CloudFormation::CustomResource')).find(([id]) => id.startsWith('BridgeRoleConditions'))
      ?? Object.entries(t.toJSON().Resources).find(([id]) => id.startsWith('BridgeRoleConditions'));
    assert(c, 'conditions resource missing');
    const v = JSON.stringify((c[1] as any).Properties.Value);
    assert(v.includes('Fn::Split') && v.includes('https://'), 'issuer must be split on https://');
    assert(v.includes(':sub') && v.includes('system:serviceaccount:skybook:aws-bridge'));
  }],
  ['WAF web ACL with managed rules + rate limit', () => t.hasResourceProperties('AWS::WAFv2::WebACL', { Scope: 'REGIONAL', Rules: Match.arrayWith([Match.objectLike({ Name: 'rate-limit-per-ip' })]) })],
  ['ECR repo with scan-on-push', () => t.hasResourceProperties('AWS::ECR::Repository', { ImageScanningConfiguration: { ScanOnPush: true } })],
  ['secrets in Secrets Manager (5 DB + JWT + internal token)', () => t.resourceCountIs('AWS::SecretsManager::Secret', SERVICE_DATABASES.length + 2)],
  ['data stores are not reachable from the internet', () => {
    t.allResourcesProperties('AWS::RDS::DBInstance', { PubliclyAccessible: false });
  }],
];

let failed = 0;
for (const [name, fn] of checks) {
  try { fn(); console.log(`PASS  ${name}`); } catch (e) { failed++; console.log(`FAIL  ${name}: ${(e as Error).message.split('\n')[0]}`); }
}
const resources = Object.keys(t.toJSON().Resources).length;
console.log(`\n${resources} CloudFormation resources synthesized; ${checks.length - failed}/${checks.length} checks passed`);
process.exit(failed ? 1 : 0);
