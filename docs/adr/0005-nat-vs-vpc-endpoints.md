# ADR 0005: One NAT gateway instead of interface VPC endpoints

- Status: Accepted
- Date: 2026-10-27

## Context

The private subnets hold Fargate tasks, Lambdas, MSK Connect workers, MSK, and RDS. Several of these need AWS APIs that are not in the VPC:

- Fargate: ECR (`ecr.api`, `ecr.dkr`) to pull images, and CloudWatch Logs (`logs`).
- Stream processor and delivery service: Glue Schema Registry API (`glue`).
- Lambdas: Secrets Manager (`secretsmanager`).
- Custom-resource Lambdas: the CloudFormation response URL (an HTTPS endpoint outside AWS APIs).

Two ways to reach these from private subnets:

1. One NAT gateway in a public subnet, with routes from the private subnets to it.
2. Interface VPC endpoints for `ecr.api`, `ecr.dkr`, `logs`, `glue`, and `secretsmanager`, one per AZ. S3 uses a gateway endpoint in both options.

Prices are from `docs/cost.md` (checked 2026-10-27). The sa-east-1 hourly prices for NAT and interface endpoints are **not readable** from the AWS pricing pages, so the comparison below states quantities, not dollar amounts. Verify in the AWS Pricing Calculator before deploy.

## Decision

Use **one NAT gateway** (in the public subnet, with its public IPv4) for the M4 demo. Use the free S3 gateway endpoint for S3 traffic. Do not create interface endpoints.

## Consequences

Positive:
- One hourly NAT charge plus per-GB data processing, compared with five interface endpoints billed per AZ-hour each (two AZs means ten endpoint-AZ-hours per hour, plus per-GB charges). The NAT option has the lower fixed hourly cost for this demo.
- Simpler template: one route target instead of five endpoint resources, security groups and private DNS settings.
- The custom-resource Lambdas reach the CloudFormation response URL, which needs internet egress; NAT covers this without a special case.
- Teardown removes the NAT gateway and its IP with the stack.

Negative:
- Traffic to AWS APIs leaves the VPC and crosses the internet gateway path. The data goes over TLS and IAM still authorizes every call, but the network path is wider than with endpoints.
- Data processed through NAT is billed per GB. Large ECR image pulls (the stream-processor and delivery images) add to this. Image sizes should stay small.
- A single NAT gateway is a single AZ dependency. If its AZ fails, private tasks lose AWS API access. Acceptable for a demo; not for production.
- The NAT gateway is the largest item to forget at teardown (see `docs/cost.md`, forgotten-teardown section).

## Production

Production would prefer interface endpoints (plus the S3 gateway endpoint) and no general internet egress. That gives least-privilege network paths, keeps AWS API traffic inside the VPC, and removes the NAT as a single point of failure. The extra hourly endpoint cost is accepted there for the security and availability benefit. The CloudFormation response URL would still need a path (a dedicated egress for the custom-resource Lambdas, or a different response mechanism).

## Alternatives considered

- **Interface endpoints only, no NAT.** Rejected for M4: five endpoints per AZ is more template and more hourly cost for a two-AZ demo, and the custom-resource Lambdas still need internet egress for the CloudFormation response URL.
- **NAT per AZ.** Rejected for M4: doubles the NAT hourly cost to remove an availability risk the demo does not need.
