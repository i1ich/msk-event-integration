package io.github.i1ich.mskei.ingress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

class SecretsTest {
    private static final String ARN = "arn:aws:secretsmanager:sa-east-1:111111111111:secret:s";

    @Test
    void arnWinsOverPlainEnvSecret() {
        SecretReader reader = id -> id.equals(ARN) ? "from-sm" : null;
        IngressConfig c = IngressConfig.from(
                Map.of("INGRESS_HMAC_SECRET_ARN", ARN, "INGRESS_HMAC_SECRET", "from-env"), reader);
        assertThat(c.hmacSecret()).isEqualTo("from-sm");
    }

    @Test
    void plainEnvSecretStillWorksAndReaderIsNotUsed() {
        SecretReader reader = id -> {
            throw new AssertionError("must not be called");
        };
        assertThat(IngressConfig.from(Map.of("INGRESS_HMAC_SECRET", "x"), reader).hmacSecret()).isEqualTo("x");
    }

    @Test
    void blankArnIsIgnored() {
        SecretReader reader = id -> {
            throw new AssertionError("must not be called");
        };
        assertThat(IngressConfig.from(Map.of("INGRESS_HMAC_SECRET_ARN", " "), reader).hmacEnabled()).isFalse();
    }

    @Test
    void emptySecretInManagerIsAnError() {
        SecretReader reader = id -> " ";
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> IngressConfig.from(Map.of("INGRESS_HMAC_SECRET_ARN", ARN), reader))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void secretsManagerReaderReturnsTheSecretString() {
        SecretsManagerClient client = mock(SecretsManagerClient.class);
        GetSecretValueRequest request = GetSecretValueRequest.builder().secretId(ARN).build();
        when(client.getSecretValue(request)).thenReturn(GetSecretValueResponse.builder().secretString("raw").build());
        assertThat(new SecretsManagerReader(client).read(ARN)).isEqualTo("raw");
        verify(client).getSecretValue(request);
    }
}
