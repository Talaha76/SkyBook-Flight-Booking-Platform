import { CfnJson, CfnOutput, Duration, Fn, RemovalPolicy, Stack, StackProps, Tags } from 'aws-cdk-lib';
import * as aps from 'aws-cdk-lib/aws-aps';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as eks from 'aws-cdk-lib/aws-eks';
import * as elasticache from 'aws-cdk-lib/aws-elasticache';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as msk from 'aws-cdk-lib/aws-msk';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as secrets from 'aws-cdk-lib/aws-secretsmanager';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as wafv2 from 'aws-cdk-lib/aws-wafv2';
import { Construct } from 'constructs';

export interface SkyBookStackProps extends StackProps {
  /** Small sizes for a demo/staging environment; production would raise these. */
  sizing?: { dbInstance: ec2.InstanceType; cacheNode: string; eksNode: string; eksNodes: number };
}

/** One RDS instance per service keeps the local "database per service" rule on AWS. */
export const SERVICE_DATABASES = ['flights', 'booking', 'inventory', 'payment', 'notification'] as const;

/**
 * SkyBook production blueprint. Local component -> AWS managed equivalent:
 *   Kafka (KRaft)        -> Amazon MSK Serverless (IAM auth)
 *   Postgres per service -> Amazon RDS for PostgreSQL (one instance per service, Secrets Manager credentials)
 *   Redis                -> Amazon ElastiCache for Redis (Multi-AZ replication group)
 *   kind + Helm          -> Amazon EKS (same Helm chart) + IRSA for pod credentials
 *   Container image      -> Amazon ECR
 *   Prometheus/Grafana   -> Amazon Managed Service for Prometheus (+ Managed Grafana, configured separately)
 *   Env-var secrets      -> AWS Secrets Manager (+ External Secrets Operator in the cluster)
 *   Gateway exposure     -> ALB (AWS Load Balancer Controller Ingress) + AWS WAF web ACL
 *   Bridge target        -> Amazon SQS flight-events queue (+ DLQ) feeding Project 2
 * Intended for `cdk synth` only. See docs/aws-cost-estimate.md before deploying anything.
 */
export class SkyBookStack extends Stack {
  constructor(scope: Construct, id: string, props: SkyBookStackProps = {}) {
    super(scope, id, props);
    const size = props.sizing ?? {
      dbInstance: ec2.InstanceType.of(ec2.InstanceClass.T4G, ec2.InstanceSize.MICRO),
      cacheNode: 'cache.t4g.micro',
      eksNode: 't3.large',
      eksNodes: 2,
    };
    Tags.of(this).add('Project', 'SkyBook');

    // ---------------------------------------------------------------- network
    const vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways: 1,
      subnetConfiguration: [
        { name: 'public', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 },
        { name: 'app', subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS, cidrMask: 22 },
        { name: 'data', subnetType: ec2.SubnetType.PRIVATE_ISOLATED, cidrMask: 24 },
      ],
    });
    const appSubnets = vpc.selectSubnets({ subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS });
    const dataSubnets = vpc.selectSubnets({ subnetType: ec2.SubnetType.PRIVATE_ISOLATED });

    const appSg = new ec2.SecurityGroup(this, 'AppSg', { vpc, description: 'SkyBook EKS worker nodes / pods' });
    const dataSg = new ec2.SecurityGroup(this, 'DataSg', { vpc, description: 'SkyBook data stores (RDS, ElastiCache, MSK)', allowAllOutbound: false });
    dataSg.addIngressRule(appSg, ec2.Port.tcp(5432), 'Postgres from app');
    dataSg.addIngressRule(appSg, ec2.Port.tcp(6379), 'Redis from app');
    dataSg.addIngressRule(appSg, ec2.Port.tcp(9098), 'MSK IAM auth from app');

    // ---------------------------------------------------------------- container registry
    const repo = new ecr.Repository(this, 'AppRepo', {
      repositoryName: 'skybook-app',
      imageScanOnPush: true,
      lifecycleRules: [{ maxImageCount: 20 }],
      removalPolicy: RemovalPolicy.RETAIN,
    });

    // ---------------------------------------------------------------- Kafka -> MSK Serverless
    const kafka = new msk.CfnServerlessCluster(this, 'Kafka', {
      clusterName: 'skybook',
      clientAuthentication: { sasl: { iam: { enabled: true } } },
      vpcConfigs: [{ subnetIds: appSubnets.subnetIds, securityGroups: [dataSg.securityGroupId] }],
    });

    // ---------------------------------------------------------------- Postgres per service -> RDS
    const databases = SERVICE_DATABASES.map(
      (name) =>
        new rds.DatabaseInstance(this, `Db-${name}`, {
          engine: rds.DatabaseInstanceEngine.postgres({ version: rds.PostgresEngineVersion.VER_16 }),
          instanceType: size.dbInstance,
          vpc,
          vpcSubnets: dataSubnets,
          securityGroups: [dataSg],
          databaseName: `skybook_${name}`,
          credentials: rds.Credentials.fromGeneratedSecret('skybook', { secretName: `skybook/${name}/db` }),
          allocatedStorage: 20,
          maxAllocatedStorage: 100,
          storageEncrypted: true,
          backupRetention: Duration.days(7),
          multiAz: false, // production: true
          deletionProtection: false, // production: true
          removalPolicy: RemovalPolicy.SNAPSHOT,
          enablePerformanceInsights: false,
        }),
    );

    // ---------------------------------------------------------------- Redis -> ElastiCache
    const cacheSubnets = new elasticache.CfnSubnetGroup(this, 'CacheSubnets', {
      description: 'SkyBook cache subnets',
      subnetIds: dataSubnets.subnetIds,
    });
    const cache = new elasticache.CfnReplicationGroup(this, 'Cache', {
      replicationGroupDescription: 'SkyBook search cache + rate limiter',
      engine: 'redis',
      engineVersion: '7.1',
      cacheNodeType: size.cacheNode,
      numCacheClusters: 2,
      automaticFailoverEnabled: true,
      multiAzEnabled: true,
      atRestEncryptionEnabled: true,
      transitEncryptionEnabled: true,
      cacheSubnetGroupName: cacheSubnets.ref,
      securityGroupIds: [dataSg.securityGroupId],
    });

    // ---------------------------------------------------------------- bridge target -> SQS (+ DLQ)
    const dlq = new sqs.Queue(this, 'FlightEventsDlq', { queueName: 'skybook-flight-events-dlq', retentionPeriod: Duration.days(14), encryption: sqs.QueueEncryption.SQS_MANAGED });
    const flightEvents = new sqs.Queue(this, 'FlightEvents', {
      queueName: 'skybook-flight-events',
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      visibilityTimeout: Duration.seconds(60),
      retentionPeriod: Duration.days(4),
      deadLetterQueue: { queue: dlq, maxReceiveCount: 5 },
      enforceSSL: true,
    });

    // ---------------------------------------------------------------- secrets
    const jwtSecret = new secrets.Secret(this, 'JwtSecret', {
      secretName: 'skybook/gateway/jwt',
      generateSecretString: { passwordLength: 48, excludePunctuation: true },
    });
    const internalToken = new secrets.Secret(this, 'InternalToken', {
      secretName: 'skybook/internal-token',
      generateSecretString: { passwordLength: 48, excludePunctuation: true },
    });

    // ---------------------------------------------------------------- EKS (L1 so no kubectl Lambda layer is needed)
    const clusterRole = new iam.Role(this, 'EksClusterRole', {
      assumedBy: new iam.ServicePrincipal('eks.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonEKSClusterPolicy')],
    });
    const cluster = new eks.CfnCluster(this, 'Eks', {
      name: 'skybook',
      version: '1.31',
      roleArn: clusterRole.roleArn,
      accessConfig: { authenticationMode: 'API_AND_CONFIG_MAP' },
      resourcesVpcConfig: {
        subnetIds: [...appSubnets.subnetIds],
        securityGroupIds: [appSg.securityGroupId],
        endpointPrivateAccess: true,
        endpointPublicAccess: true,
      },
      logging: { clusterLogging: { enabledTypes: [{ type: 'api' }, { type: 'audit' }] } },
    });
    const nodeRole = new iam.Role(this, 'EksNodeRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: ['AmazonEKSWorkerNodePolicy', 'AmazonEKS_CNI_Policy', 'AmazonEC2ContainerRegistryReadOnly'].map((p) =>
        iam.ManagedPolicy.fromAwsManagedPolicyName(p),
      ),
    });
    new eks.CfnNodegroup(this, 'EksNodes', {
      clusterName: cluster.ref,
      nodeRole: nodeRole.roleArn,
      subnets: appSubnets.subnetIds,
      instanceTypes: [size.eksNode],
      scalingConfig: { minSize: size.eksNodes, desiredSize: size.eksNodes, maxSize: size.eksNodes * 2 },
      capacityType: 'ON_DEMAND',
      labels: { app: 'skybook' },
    });

    // IRSA: Kubernetes service accounts get scoped AWS roles - no access keys in pods.
    const issuer = cluster.attrOpenIdConnectIssuerUrl;
    const oidc = new iam.OpenIdConnectProvider(this, 'EksOidc', { url: issuer, clientIds: ['sts.amazonaws.com'] });
    // Condition keys must be the issuer WITHOUT "https://"; the issuer is a deploy-time token, so strip it with Fn::Split.
    const issuerHost = Fn.select(1, Fn.split('https://', issuer));
    const irsaRole = (id: string, serviceAccount: string) => {
      const conditions = new CfnJson(this, `${id}Conditions`, {
        value: {
          [`${issuerHost}:sub`]: `system:serviceaccount:skybook:${serviceAccount}`,
          [`${issuerHost}:aud`]: 'sts.amazonaws.com',
        },
      });
      return new iam.Role(this, id, {
        roleName: `skybook-${serviceAccount}`,
        assumedBy: new iam.FederatedPrincipal(oidc.openIdConnectProviderArn, { StringEquals: conditions }, 'sts:AssumeRoleWithWebIdentity'),
      });
    };

    // Least privilege: the bridge can ONLY send to the one flight-events queue.
    const bridgeRole = irsaRole('BridgeRole', 'aws-bridge');
    bridgeRole.addToPolicy(new iam.PolicyStatement({ actions: ['sqs:SendMessage'], resources: [flightEvents.queueArn] }));

    // Every service may connect to MSK; only the topics it uses would be scoped in a hardened version.
    const appRole = irsaRole('AppRole', 'skybook-app');
    appRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['kafka-cluster:Connect', 'kafka-cluster:DescribeCluster', 'kafka-cluster:*Topic*', 'kafka-cluster:ReadData', 'kafka-cluster:WriteData', 'kafka-cluster:AlterGroup', 'kafka-cluster:DescribeGroup'],
        resources: [kafka.attrArn, `arn:aws:kafka:${this.region}:${this.account}:topic/skybook/*`, `arn:aws:kafka:${this.region}:${this.account}:group/skybook/*`],
      }),
    );
    for (const db of databases) db.secret?.grantRead(appRole);
    jwtSecret.grantRead(appRole);
    internalToken.grantRead(appRole);
    bridgeRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['kafka-cluster:Connect', 'kafka-cluster:DescribeCluster', 'kafka-cluster:ReadData', 'kafka-cluster:DescribeTopic', 'kafka-cluster:AlterGroup', 'kafka-cluster:DescribeGroup'],
        resources: [kafka.attrArn, `arn:aws:kafka:${this.region}:${this.account}:topic/skybook/*`, `arn:aws:kafka:${this.region}:${this.account}:group/skybook/*`],
      }),
    );

    // ---------------------------------------------------------------- edge protection: WAF for the gateway ALB
    const waf = new wafv2.CfnWebACL(this, 'GatewayWaf', {
      name: 'skybook-gateway',
      scope: 'REGIONAL',
      defaultAction: { allow: {} },
      visibilityConfig: { cloudWatchMetricsEnabled: true, metricName: 'skybook-gateway', sampledRequestsEnabled: true },
      rules: [
        {
          name: 'aws-common',
          priority: 0,
          overrideAction: { none: {} },
          statement: { managedRuleGroupStatement: { vendorName: 'AWS', name: 'AWSManagedRulesCommonRuleSet' } },
          visibilityConfig: { cloudWatchMetricsEnabled: true, metricName: 'aws-common', sampledRequestsEnabled: true },
        },
        {
          name: 'rate-limit-per-ip',
          priority: 1,
          action: { block: {} },
          statement: { rateBasedStatement: { limit: 2000, aggregateKeyType: 'IP' } },
          visibilityConfig: { cloudWatchMetricsEnabled: true, metricName: 'rate-limit-per-ip', sampledRequestsEnabled: true },
        },
      ],
    });

    // ---------------------------------------------------------------- monitoring
    const prom = new aps.CfnWorkspace(this, 'Prometheus', { alias: 'skybook' });

    // ---------------------------------------------------------------- outputs (feed these into Helm values)
    new CfnOutput(this, 'EcrRepositoryUri', { value: repo.repositoryUri });
    new CfnOutput(this, 'MskClusterArn', { value: kafka.attrArn });
    new CfnOutput(this, 'RedisPrimaryEndpoint', { value: cache.attrPrimaryEndPointAddress });
    databases.forEach((db, i) => new CfnOutput(this, `DbEndpoint${SERVICE_DATABASES[i]}`, { value: db.dbInstanceEndpointAddress }));
    new CfnOutput(this, 'FlightEventsQueueUrl', { value: flightEvents.queueUrl });
    new CfnOutput(this, 'BridgeRoleArn', { value: bridgeRole.roleArn, description: 'annotate the aws-bridge ServiceAccount with eks.amazonaws.com/role-arn' });
    new CfnOutput(this, 'AppRoleArn', { value: appRole.roleArn });
    new CfnOutput(this, 'GatewayWafArn', { value: waf.attrArn, description: 'alb.ingress.kubernetes.io/wafv2-acl-arn on the gateway Ingress' });
    new CfnOutput(this, 'PrometheusWorkspaceId', { value: prom.attrWorkspaceId });
    new CfnOutput(this, 'EksClusterName', { value: cluster.ref });
  }
}
