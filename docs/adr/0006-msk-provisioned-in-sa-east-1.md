# ADR 0006: MSK provisioned cluster in sa-east-1

- Status: Accepted
- Date: 2026-10-27

## Context

The default region is sa-east-1 (São Paulo). MSK Serverless is not offered there. The MSK Serverless documentation lists its available regions, and sa-east-1 is not among them (source: https://docs.aws.amazon.com/msk/latest/developerguide/serverless.html, checked 2026-10-27). The M4 deployment therefore uses a provisioned cluster.

The deployment must still work with the Kafka features the platform uses: `exactly_once_v2` in Kafka Streams, IAM authentication (ADR 0004), and retry topics (ADR 0003).

## Decision

- **Provisioned cluster: 2 × `kafka.t3.small`**, one broker per AZ, in the two private subnets.
- **Custom `AWS::MSK::Configuration`** with replication settings sized for two brokers:
  - `default.replication.factor=2`
  - `min.insync.replicas=1`
  - `offsets.topic.replication.factor=2`
  - `transaction.state.log.replication.factor=2`
  - `transaction.state.log.min.isr=1`
  - `auto.create.topics.enable=false`, `num.partitions=3`
- **Transaction state log at RF 2** is required for `exactly_once_v2`. The broker default (RF 3) cannot be met with two brokers.
- The template keeps `ClusterType=serverless` as a parameter option for regions that support it. `deploy.sh` refuses `serverless` in sa-east-1.

## Consequences

Positive:
- Works in the default region with the same IAM and Kafka client code as Serverless.
- Fixed hourly cost, which is easy to budget and easy to see on the bill (broker-hours and storage).

Negative:
- **Fixed cost while running.** Brokers are billed per hour whether or not traffic flows. This is the main cost risk and the reason for the same-day teardown rule (`docs/cost.md`).
- **Operations are ours.** Broker patching, storage sizing and version upgrades are on the team, not AWS.
- **Partition limits on `kafka.t3.small`.** The small instance type has limits on partitions and connections. The topic count and `num.partitions=3` stay within demo load, but larger topologies need a bigger broker type.
- **`min.insync.replicas=1` with `acks=all`** lets producers continue while one broker is down. This is a demo trade-off: a write acknowledged by only one replica can be lost if that broker fails before replication. Production uses 3 brokers, RF 3 and min ISR 2.
- **Two brokers means no rolling-restart headroom** for replication. Maintenance temporarily leaves one replica per partition.

## Alternatives considered

- **MSK Serverless.** Not available in sa-east-1. In regions where it is, it would remove broker sizing and patching and bill per throughput. Kept as a template option, not the default.
- **Three brokers (RF 3, min ISR 2).** Matches the production profile and survives one broker failure with no write-loss risk at min ISR 2. Rejected for M4: one more broker-hour running all day, and the demo does not need the availability. Recorded as the production target.
- **Single broker.** Rejected: no replication, so a broker loss stops all producers and consumers, and the transaction state log cannot have RF 2.
