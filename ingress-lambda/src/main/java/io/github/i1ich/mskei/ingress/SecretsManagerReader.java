package io.github.i1ich.mskei.ingress;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;

/**
 * {@link SecretReader} backed by AWS Secrets Manager. Region and credentials come from the Lambda environment
 * (AWS_REGION, execution role). The URL-connection HTTP client keeps the jar small and the cold start fast.
 */
public final class SecretsManagerReader implements SecretReader {
    private final SecretsManagerClient client;

    public SecretsManagerReader() {
        this(SecretsManagerClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build());
    }

    /** For tests: inject a mock client. */
    SecretsManagerReader(SecretsManagerClient client) {
        this.client = client;
    }

    @Override
    public String read(String secretId) {
        return client.getSecretValue(GetSecretValueRequest.builder().secretId(secretId).build()).secretString();
    }
}
