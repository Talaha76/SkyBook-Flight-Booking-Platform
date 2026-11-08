# AWS cost estimate for the production blueprint

> **Nothing here is deployed.** `infra/aws-blueprint` is synth-only (`npm run synth`). This estimate is what the
> blueprint *would* cost per month if it were deployed as written. Use it to talk through trade-offs in an interview, and
> confirm the numbers in the [AWS Pricing Calculator](https://calculator.aws/) for your region (the owner's project is in `us-east-2`) before spending anything.

**Method.** Published on-demand **us-east-1** rates (checked 2026-11-07, sources below) × 730 hours/month.
us-east-2 (Ohio) generally has the same rates as us-east-1. The estimate assumes light demo traffic, so data transfer, storage and
request charges are small and listed separately.

## Blueprint as written (`lib/skybook-stack.ts`)

| Component | Blueprint sizing | Rate | Monthly |
|---|---|---|---|
| **Amazon MSK Serverless** | 1 cluster | $0.75 / cluster-hour | **$547.50** |
|  | 7 topics × 3 partitions = 21 partitions | $0.0015 / partition-hour | $23.00 |
| Amazon EKS control plane | 1 cluster, standard support | $0.10 / hour | $73.00 |
| EKS worker nodes | 2 × t3.large | $0.0832 / hour each | $121.47 |
| Amazon RDS for PostgreSQL | 5 × db.t4g.micro, Single-AZ | $0.016 / hour each | $58.40 |
| Amazon ElastiCache (Redis) | 2 × cache.t4g.micro (primary + replica) | $0.016 / hour each | $23.36 |
| NAT gateway | 1 | $0.045 / hour (+ $0.045 / GB processed) | $32.85 |
| Application Load Balancer | 1 (created by the AWS Load Balancer Controller) | $0.0225 / hour + ~1 LCU at $0.008 / LCU-hour | $22.27 |
| AWS WAF | 1 web ACL + 2 rules, ~1M requests | $5 / ACL + $1 / rule + $0.60 / 1M requests | $7.60 |
| Public IPv4 addresses | ~3 (ALB × 2, NAT × 1) | $0.005 / hour each | $10.95 |
| Amazon SQS | flight-events + DLQ | first 1M requests/month free | $0.00 |
| AWS Secrets Manager | 7 secrets | $0.40 / secret-month (not re-checked) | ~$2.80 |
| **Total (before storage, data transfer, Prometheus ingest)** | | | **≈ $923 / month** |

Not included: RDS storage (5 × 20 GB gp2/gp3), EBS volumes for the nodes, Amazon Managed Service for Prometheus
ingestion/storage (usage-based), CloudWatch logs, and inter-AZ / internet data transfer. For a demo these add tens of dollars, not hundreds.

## What the estimate says (interview talking points)

1. **MSK Serverless is about 62 % of the bill.** That's $570 of $923, and it comes from the cluster-hour charge alone, whatever the traffic.
   For a low-traffic system, **MSK Provisioned with 2 × kafka.t3.small brokers** costs about **$67/month** in broker
   hours ($0.0456/hour each) plus storage, and brings the total to about **$420/month**. The trade-off is that you size and patch brokers
   yourself, and t3.small has throughput limits. Serverless pays off once you'd otherwise need m5/m7g brokers or traffic is very spiky.
2. **The fixed platform tax is about $140/month** before any workload: EKS $73 + NAT $33 + ALB $16 + IPv4 $11 + WAF $7.
   ECS Fargate would remove the $73 EKS fee. The guide's alternative, k3s on one EC2 instance, removes most of it.
3. **One RDS instance per service costs 5×.** Five db.t4g.micro instances cost $58. A single db.t4g.small hosting five databases would cost
   about half, but it gives up independent scaling, maintenance windows and blast radius, which is the point of database-per-service.
   Production would also turn on Multi-AZ (roughly doubling RDS) and Reserved Instances (≈ 30 % off).
4. **ElastiCache Multi-AZ doubles the node cost** ($11.68 → $23.36), and it's worth it: the gateway rate limiter fails open
   and search falls back to Postgres, but losing Redis still means a hit-rate cliff.
5. **NAT data processing is the classic surprise bill** ($0.045/GB on top of egress). Adding VPC endpoints for ECR, S3, SQS and Secrets Manager
   keeps that traffic off the NAT.
6. **The bridge's SQS usage is effectively free.** `SendMessageBatch` packs 10 events per request, so 1M free requests covers about 10M events/month.

## Cheapest way to prove it works on AWS (what the guide recommends)

Run only the **Kafka → SQS bridge against a real SQS queue** (about $0: free tier), and keep everything else local. See
[aws-setup.md](aws-setup.md). Optionally, run a few-hour EC2 + k3s proof using the Helm chart (under $1, then tear it down).

## Sources (rates as published, us-east-1)

- MSK Serverless: $0.75/cluster-hour, $0.0015/partition-hour, $0.10/GB in, $0.05/GB out. [instaclustr summary of the AWS page](https://www.instaclustr.com/education/apache-kafka/apache-kafka-on-aws-features-pricing-tutorial-and-best-practices/), [AutoMQ, checked against AWS on 2026-05-20](https://www.automq.com/blog/aws-msk-pricing-provisioned-serverless-express)
- MSK Provisioned kafka.t3.small: $0.0456/hour. [instaclustr](https://www.instaclustr.com/?p=14890)
- EKS: $0.10 per cluster per hour (standard support). [aws.amazon.com/eks/pricing](https://aws.amazon.com/eks/pricing/)
- RDS db.t4g.micro PostgreSQL: $0.016/hour. [vantage](https://instances.vantage.sh/aws/rds/db.t4g.micro)
- ElastiCache cache.t4g.micro: $0.016/hour. [vantage](https://instances.vantage.sh/aws/elasticache/cache.t4g.micro)
- EC2 t3.large: $0.0832/hour. [doit compute](https://compute.doit.com/spot/us-east-1/t3.large)
- NAT gateway: $0.045/hour + $0.045/GB. [awsfundamentals](https://awsfundamentals.com/blog/avoid-nat-gateway-costs)
- Public IPv4: $0.005/hour. [awsfundamentals](https://awsfundamentals.com/blog/avoid-nat-gateway-costs)
- ALB: $0.0225/hour + $0.008/LCU-hour. [costbench](https://costbench.com/software/load-balancers/aws-alb-nlb)
- WAF: $5/web ACL, $1/rule, $0.60 per 1M requests. [aws.amazon.com/waf/pricing](https://aws.amazon.com/waf/pricing)
