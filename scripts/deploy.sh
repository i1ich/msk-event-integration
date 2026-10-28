#!/usr/bin/env bash
# Deploy the whole stack to AWS in contract order (infra/cloudformation/CONTRACT.md).
# Usage: scripts/deploy.sh [env] [--yes] [--skip-build] [--no-budget-check] [--cluster-type provisioned|serverless]
#   env              environment name, default dev (or $ENV_NAME)
#   --yes            do not ask for confirmation (CI)
#   --skip-build     reuse existing target/*.jar and the plugin zip
#   --no-budget-check  do not require the msk-ei-budget stack (local experiments only, not in CI)
# Environment: AWS_REGION (default sa-east-1), CLUSTER_TYPE (default provisioned), ALERT_EMAIL (optional, observability
# alarms), TOPIC_PARTITIONS (optional override), PLUGIN_ZIP (optional path of the Connect plugin zip).
# Costs money while it runs: tear down the same day with scripts/teardown.sh.
set -euo pipefail

ENV_NAME="${ENV_NAME:-dev}"
CLUSTER_TYPE="${CLUSTER_TYPE:-provisioned}"
SKIP_BUILD=false
BUDGET_CHECK=true

POSITIONAL=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --yes)              ASSUME_YES=true ;;
    --skip-build)       SKIP_BUILD=true ;;
    --no-budget-check)  BUDGET_CHECK=false ;;
    --cluster-type)     shift; CLUSTER_TYPE="${1:?--cluster-type needs a value}" ;;
    -h|--help)          sed -n '2,12p' "$0"; exit 0 ;;
    --*)                echo "unknown option: $1" >&2; exit 2 ;;
    *)                  POSITIONAL+=("$1") ;;
  esac
  shift
done
[ "${#POSITIONAL[@]}" -gt 0 ] && ENV_NAME="${POSITIONAL[0]}"
export ENV_NAME ASSUME_YES

# shellcheck source=lib/aws-common.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib/aws-common.sh"

# Regions with MSK Serverless. Keep this list small and conservative; verify before adding a region:
# https://docs.aws.amazon.com/msk/latest/developerguide/serverless.html (see "Supported Regions").
# sa-east-1 is NOT in the list (checked 2026-10-27), which is why the default is provisioned.
MSK_SERVERLESS_REGIONS=" us-east-1 us-east-2 us-west-2 eu-west-1 eu-central-1 eu-north-1 ap-southeast-1 ap-southeast-2 ap-northeast-1 ap-south-1 "

# ---------------------------------------------------------------------------------------------------------------
# Parameters per stack: edit HERE if a template parameter changes. One function per stack, one Key=Value per line.
# Globals available: ENV_NAME, CLUSTER_TYPE, BOOTSTRAP_SERVERS, SHA, KEY_BOOTSTRAP, KEY_INGRESS, KEY_PLUGIN,
#                    IMG_STREAM, IMG_DELIVERY, IMG_MOCK, ALERT_EMAIL.
# ---------------------------------------------------------------------------------------------------------------
params_artifacts()       { echo "EnvName=$ENV_NAME"; }
params_network()         { echo "EnvName=$ENV_NAME"; }
params_msk()             { echo "EnvName=$ENV_NAME"; echo "ClusterType=$CLUSTER_TYPE"; }
params_topics() {
  echo "EnvName=$ENV_NAME"
  echo "BootstrapServers=$BOOTSTRAP_SERVERS"
  echo "BootstrapArtifactKey=$KEY_BOOTSTRAP"
  if [ -n "${TOPIC_PARTITIONS:-}" ]; then echo "TopicPartitions=$TOPIC_PARTITIONS"; fi
}
params_schema_registry() { echo "EnvName=$ENV_NAME"; }
params_database()        { echo "EnvName=$ENV_NAME"; echo "BootstrapArtifactKey=$KEY_BOOTSTRAP"; }
params_ecr()             { echo "EnvName=$ENV_NAME"; }
params_msk_connect()     { echo "EnvName=$ENV_NAME"; echo "BootstrapServers=$BOOTSTRAP_SERVERS"; echo "PluginArtifactKey=$KEY_PLUGIN"; }
params_ingress() {
  echo "EnvName=$ENV_NAME"
  echo "BootstrapServers=$BOOTSTRAP_SERVERS"
  echo "IngressArtifactKey=$KEY_INGRESS"
  echo "AlarmTopicArn=$ALARM_TOPIC_ARN"
  echo "ConfigureApiGatewayAccountRole=$APIGW_ACCOUNT_ROLE"
}
params_processing() {
  echo "EnvName=$ENV_NAME"
  echo "BootstrapServers=$BOOTSTRAP_SERVERS"
  echo "StreamProcessorImage=$IMG_STREAM"
  echo "DeliveryServiceImage=$IMG_DELIVERY"
  echo "DownstreamMockImage=$IMG_MOCK"
}
params_observability() {
  echo "EnvName=$ENV_NAME"
  if [ -n "$ALERT_EMAIL" ]; then echo "AlarmEmail=$ALERT_EMAIL"; fi
}

# deploy_with <template> <stack suffix> <params function>
deploy_with() {
  local params=()
  mapfile -t params < <("$3")
  deploy_stack "$1" "$(stack_name "$2")" "${params[@]}"
  TIMINGS+=("$(printf '%-16s %s' "$2" "done")")
}

TIMINGS=()
ALERT_EMAIL="${ALERT_EMAIL:-}"
BOOTSTRAP_SERVERS=""
START_ALL=$SECONDS

# ---- Preflight -------------------------------------------------------------------------------------------------
validate_env_name
log "Preflight"
require_cmd aws docker mvn java git openssl
case "$CLUSTER_TYPE" in provisioned|serverless) ;; *) die "CLUSTER_TYPE must be provisioned or serverless" ;; esac
if [ "$CLUSTER_TYPE" = "serverless" ] && [[ "$MSK_SERVERLESS_REGIONS" != *" $AWS_REGION "* ]]; then
  die "MSK Serverless is not available in $AWS_REGION. Use --cluster-type provisioned (or a supported region)."
fi
docker info >/dev/null 2>&1 || die "docker daemon is not running"
print_identity

if [ "$BUDGET_CHECK" = "true" ]; then
  stack_exists "$BUDGET_STACK" || die "budget stack '$BUDGET_STACK' not found. Run scripts/create-budget.sh <email> first (or --no-budget-check)."
  info "budget stack present"
else
  warn "budget check skipped"
fi

cat <<EOF

COST WARNING: this creates billable resources in $AWS_REGION (MSK $CLUSTER_TYPE, NAT gateway, RDS, Fargate, MSK Connect).
Expect roughly the hourly/daily figures in docs/cost.md; it keeps charging until you run:
    scripts/teardown.sh $ENV_NAME
EOF
confirm "Deploy env '$ENV_NAME' with ClusterType=$CLUSTER_TYPE?" || die "aborted by user"

SHA="$(git_sha)"
KEY_BOOTSTRAP="lambda/infra-bootstrap-${SHA}.jar"
KEY_INGRESS="lambda/ingress-lambda-${SHA}.jar"
KEY_PLUGIN="connect/debezium-postgres-glue-${SHA}.zip"
info "artifact version: $SHA"

# ---- Build -----------------------------------------------------------------------------------------------------
if [ "$SKIP_BUILD" = "true" ]; then
  log "Build skipped (--skip-build)"
else
  log "Build: mvn package (tests are run by CI, not here)"
  mvn -B -q package -DskipTests
  log "Build: Connect plugin zip"
  bash scripts/build-connect-plugin.sh
fi
PLUGIN_ZIP="${PLUGIN_ZIP:-infra/connect/plugin/target/debezium-postgres-glue.zip}"
JAR_INGRESS="ingress-lambda/target/ingress-lambda.jar"
JAR_BOOTSTRAP="infra-bootstrap/target/infra-bootstrap.jar"
for f in "$JAR_INGRESS" "$JAR_BOOTSTRAP" "$PLUGIN_ZIP"; do
  [ -f "$f" ] || die "missing build output: $f"
done

# ---- 1 artifacts bucket + uploads ----------------------------------------------------------------------------
deploy_with artifacts.yaml artifacts params_artifacts
BUCKET="$(export_value ArtifactsBucket)"
[ -n "$BUCKET" ] && [ "$BUCKET" != "None" ] || die "export ${ENV_NAME}-msk-ei-ArtifactsBucket not found"
log "Uploading artifacts to s3://$BUCKET"
aws s3 cp "$JAR_BOOTSTRAP" "s3://$BUCKET/$KEY_BOOTSTRAP" --only-show-errors
aws s3 cp "$JAR_INGRESS"   "s3://$BUCKET/$KEY_INGRESS"   --only-show-errors
aws s3 cp "$PLUGIN_ZIP"    "s3://$BUCKET/$KEY_PLUGIN"    --only-show-errors

# ---- 2-3 network, msk, bootstrap servers --------------------------------------------------------------------
deploy_with network.yaml network params_network
deploy_with msk.yaml msk params_msk

log "Reading bootstrap servers (SASL/IAM) from the cluster"
CLUSTER_ARN="$(stack_output "$(stack_name msk)" ClusterArn)"
[ -n "$CLUSTER_ARN" ] && [ "$CLUSTER_ARN" != "None" ] || CLUSTER_ARN="$(export_value ClusterArn)"
BOOTSTRAP_SERVERS="$(aws kafka get-bootstrap-brokers --cluster-arn "$CLUSTER_ARN" \
  --query BootstrapBrokerStringSaslIam --output text)"
[ -n "$BOOTSTRAP_SERVERS" ] && [ "$BOOTSTRAP_SERVERS" != "None" ] || die "no SASL/IAM bootstrap brokers returned"
info "bootstrap: $BOOTSTRAP_SERVERS"

# ---- 4-6 topics, schema registry, database ------------------------------------------------------------------
deploy_with topics.yaml topics params_topics
deploy_with schema-registry.yaml schema-registry params_schema_registry
bash scripts/register-schemas-glue.sh "$ENV_NAME"
deploy_with database.yaml database params_database

# ---- 7 ECR + images -----------------------------------------------------------------------------------------
deploy_with ecr.yaml ecr params_ecr
REPO_STREAM="$(export_value RepoStreamProcessor)"
REPO_DELIVERY="$(export_value RepoDeliveryService)"
REPO_MOCK="$(export_value RepoDownstreamMock)"
for r in "$REPO_STREAM" "$REPO_DELIVERY" "$REPO_MOCK"; do
  [ -n "$r" ] && [ "$r" != "None" ] || die "ECR repository export missing (RepoStreamProcessor/RepoDeliveryService/RepoDownstreamMock)"
done
IMG_STREAM="$REPO_STREAM:$SHA"
IMG_DELIVERY="$REPO_DELIVERY:$SHA"
IMG_MOCK="$REPO_MOCK:$SHA"

log "ECR login and image build/push (linux/amd64)"
aws ecr get-login-password | docker login --username AWS --password-stdin "${REPO_STREAM%%/*}"
build_push() { # <image> <dockerfile>
  info "docker build $1"
  docker build --platform linux/amd64 -f "$2" -t "$1" .
  docker push "$1"
}
build_push "$IMG_STREAM"   stream-processor/Dockerfile
build_push "$IMG_DELIVERY" delivery-service/Dockerfile
build_push "$IMG_MOCK"     infra/wiremock/Dockerfile

# ---- 8-11 connect, ingress, processing, observability -------------------------------------------------------
deploy_with msk-connect.yaml msk-connect params_msk_connect
# Observability before ingress: the ingress alarms publish to its SNS topic.
deploy_with observability.yaml observability params_observability
ALARM_TOPIC_ARN="$(stack_output "$(stack_name observability)" AlarmTopicArn)"
[ "$ALARM_TOPIC_ARN" != "None" ] || ALARM_TOPIC_ARN=""
# API Gateway's CloudWatch Logs role is ONE setting per account and region, shared with every other API in it.
# Only create it if the account has none; never take over (and later delete) a role other projects rely on.
if [ -z "$(aws apigateway get-account --query cloudwatchRoleArn --output text 2>/dev/null | grep -v '^None$' || true)" ]; then
  APIGW_ACCOUNT_ROLE=true
  info "no API Gateway CloudWatch role in this account/region: the ingress stack creates one"
else
  APIGW_ACCOUNT_ROLE=false
  info "API Gateway CloudWatch role already configured in this account/region: keeping it"
fi
deploy_with ingress.yaml ingress params_ingress
deploy_with processing.yaml processing params_processing

# ---- Smoke test ---------------------------------------------------------------------------------------------
log "Smoke test"
SMOKE_RC=0
bash scripts/smoke-test.sh "$ENV_NAME" || SMOKE_RC=$?

# ---- Summary ------------------------------------------------------------------------------------------------
ELAPSED=$((SECONDS - START_ALL))
echo
echo "================ SUMMARY ================"
echo "env:         $ENV_NAME ($AWS_REGION), ClusterType=$CLUSTER_TYPE, version $SHA"
echo "API URL:     $(stack_output "$(stack_name ingress)" ApiUrl)"
echo "Dashboard:   $(aws cloudformation describe-stack-resources --stack-name "$(stack_name observability)" \
  --query "StackResources[?ResourceType=='AWS::CloudWatch::Dashboard'].PhysicalResourceId | [0]" --output text)  (CloudWatch > Dashboards)"
echo "Total time:  $((ELAPSED / 60)) min $((ELAPSED % 60)) s"
echo "Smoke test:  $([ "$SMOKE_RC" -eq 0 ] && echo PASS || echo FAIL)"
echo "Tear down:   scripts/teardown.sh $ENV_NAME      (then scripts/check-leftovers.sh $ENV_NAME)"
echo "========================================="
exit "$SMOKE_RC"
