# Downstream mock for the AWS deployment (sidecar of delivery-service). Same mappings as the local demo.
# Build context = repo root:  docker build -f infra/wiremock/Dockerfile -t downstream-mock .
FROM wiremock/wiremock:3.10.0
COPY infra/wiremock/mappings /home/wiremock/mappings
