package io.github.i1ich.mskei.ingress;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runtime settings of the ingress.
 *
 * @param maxBodyBytes requests bigger than this are rejected with 413
 * @param hmacSecret   shared secret for the X-Signature check; null/blank means "no signature required"
 */
public record IngressConfig(int maxBodyBytes, String hmacSecret) {
    public static final String ENV_MAX_BODY_BYTES = "INGRESS_MAX_BODY_BYTES";
    public static final String ENV_HMAC_SECRET = "INGRESS_HMAC_SECRET";
    public static final String ENV_HMAC_SECRET_ARN = "INGRESS_HMAC_SECRET_ARN";
    public static final int DEFAULT_MAX_BODY_BYTES = 262_144;

    private static final Logger log = LoggerFactory.getLogger(IngressConfig.class);

    /**
     * Reads settings from the environment. On AWS the HMAC secret comes from Secrets Manager
     * (INGRESS_HMAC_SECRET_ARN); this is called once per cold start from the handler's static holder.
     * The Secrets Manager client is only created if the ARN is set.
     */
    public static IngressConfig fromEnv() {
        return from(System.getenv(), new LazySecretReader());
    }

    /** Env only, no Secrets Manager (the ARN variable is ignored). Used by local runs and tests. */
    public static IngressConfig from(Map<String, String> env) {
        return from(env, id -> {
            throw new IllegalStateException(ENV_HMAC_SECRET_ARN + " needs a SecretReader");
        });
    }

    /**
     * If INGRESS_HMAC_SECRET_ARN is set it wins over INGRESS_HMAC_SECRET. The secret string is the raw secret
     * (CloudFormation GenerateSecretString without a JSON template). Only the SOURCE is logged, never the value.
     */
    public static IngressConfig from(Map<String, String> env, SecretReader secrets) {
        String max = env.get(ENV_MAX_BODY_BYTES);
        int maxBytes = (max == null || max.isBlank()) ? DEFAULT_MAX_BODY_BYTES : Integer.parseInt(max.trim());

        String arn = env.get(ENV_HMAC_SECRET_ARN);
        String secret;
        if (arn != null && !arn.isBlank()) {
            secret = secrets.read(arn.trim());
            if (secret == null || secret.isBlank()) {
                throw new IllegalStateException("Secret " + ENV_HMAC_SECRET_ARN + " is empty");
            }
            log.info("HMAC secret source: Secrets Manager");
        } else {
            secret = env.get(ENV_HMAC_SECRET);
            log.info("HMAC secret source: {}", secret == null || secret.isBlank() ? "none (signature not required)"
                    : "environment variable");
        }
        return new IngressConfig(maxBytes, secret);
    }

    /** Creates the real Secrets Manager reader on first use, so local runs and tests never load the AWS SDK. */
    private static final class LazySecretReader implements SecretReader {
        @Override
        public String read(String secretId) {
            return new SecretsManagerReader().read(secretId);
        }
    }

    public boolean hmacEnabled() {
        return hmacSecret != null && !hmacSecret.isBlank();
    }
}
