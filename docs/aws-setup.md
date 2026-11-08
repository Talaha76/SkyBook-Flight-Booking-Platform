# Running the Kafka → SQS bridge

The bridge (`services/aws-bridge`) consumes `booking.events` and `inventory.events` and forwards each event to
Amazon SQS as a versioned contract (`skybook.booking.confirmed.v1`, `skybook.seat.held.v1`, …).

How it guarantees no lost events:
- **It commits Kafka offsets only after AWS confirms.** A chunk of up to 10 events is resolved only once `SendMessageBatch` returns them as `Successful`.
- **It retries partial failures.** Entries in the response's `Failed[]` are retried on their own, with exponential backoff.
- **It has a circuit breaker.** If AWS keeps failing, the bridge pauses its Kafka partitions (`consumer.pause()`), seeks back to the first unacknowledged
  event, and probes again with a growing cooldown. Kafka retention holds the backlog. The first successful send resumes every partition at once.
- **Downstream can deduplicate.** The Kafka `eventId` is copied into an SQS message attribute, so Project 2 can drop the rare duplicate that at-least-once delivery allows.
- **It strips sensitive data.** `paymentToken` is removed before anything leaves Kafka.

## Option A: no AWS account (fake SQS)

```powershell
docker compose --profile aws up -d                                  # adds fake-sqs + aws-bridge to the running stack
docker compose --profile tools run --rm e2e                         # generate some events
curl.exe http://localhost:4566/stats                                # {"received":..,"unique":..,"duplicates":0}
docker compose --profile aws --profile tools run --rm bridge-outage-test   # 2-minute AWS outage test (about 3.5 min)
```

Expected end of the outage test: `PASS: N/N events delivered, none lost`.

## Option B: real AWS (free tier, about $0)

These steps match AWS's **new project-based sign-up** (2026), where you sign in with `aws login`. You don't create IAM users or
access keys, and you don't need "advanced features", so don't activate them.

**One-time setup:**

1. **Create the queue.** In the console for your project, check your project's region in the top right (for example **US East (Ohio) us-east-2**).
   Then go to SQS → *Create queue* → **Standard** → name `skybook-flight-events` → *Create queue*. Copy its **URL**.
2. **Sign in from the CLI** (installed with `winget install -e --id Amazon.AWSCLI`):
   ```powershell
   aws configure set region us-east-2 --profile skybook       # use your project's region
   aws login --profile skybook --region us-east-2              # browser opens; pick your project session
   aws sts get-caller-identity --profile skybook               # prints your account + role
   ```
3. Create `.env` in the project folder:
   ```
   SQS_QUEUE_URL=https://sqs.us-east-2.amazonaws.com/<your-account-id>/skybook-flight-events
   AWS_REGION=us-east-2
   ```

The bridge container reads the `skybook` login session from your `%USERPROFILE%\.aws` folder and refreshes the short-lived
credentials itself. They last until the session ends, up to 12 hours; after that, run `aws login --profile skybook` again.
If you use classic access keys instead (`aws configure --profile skybook`), it works the same way.

**Run:**

```powershell
docker compose -f docker-compose.yml -f docker-compose.aws.yml --profile aws up -d aws-bridge
docker compose logs --tail 20 aws-bridge     # "aws-bridge listening" with endpoint "aws", and no errors
docker compose --profile tools run --rm e2e  # creates bookings -> events
```

Check in the console: SQS → `skybook-flight-events` → *Send and receive messages* → *Poll for messages*. You should see bodies like
`{"contract":"skybook.booking.confirmed.v1", ...}`.

**Real-AWS outage test (bullet 5).** The script tells you when to switch your Wi-Fi off for 2 minutes and when to turn it back on.
It reads and deletes messages from the queue to count them, so use this test queue:

```powershell
docker compose -f docker-compose.yml -f docker-compose.aws.yml --profile aws --profile tools run --rm bridge-outage-test
```

**Least privilege.** In the new sign-up experience, AWS's IAM Role Manager manages your project's permissions. The send-only policy
(`infra/iam/bridge-policy.json`) is what you'd attach in a classic account or on EKS (see below); the CDK blueprint creates that role.

**Clean up:** delete the queue when you're done. An idle Standard queue costs nothing anyway.

## On EKS (blueprint)

The CDK blueprint creates the role `skybook-aws-bridge`, which allows only `sqs:SendMessage` on the one queue and can be assumed only through IRSA by the
`skybook/aws-bridge` service account. Install the Helm chart with:

```
--set awsBridge.enabled=true \
--set awsBridge.queueUrl=<FlightEventsQueueUrl> \
--set awsBridge.serviceAccountAnnotations."eks\.amazonaws\.com/role-arn"=<BridgeRoleArn>
```

No access keys exist anywhere.
