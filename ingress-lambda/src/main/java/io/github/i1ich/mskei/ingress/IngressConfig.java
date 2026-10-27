package io.github.i1ich.mskei.ingress;

import java.util.Map;

/**
 * Runtime settings of the ingress.
 *
 * @param maxBodyBytes requests bigger than this are rejected with 413
 * @param hmacSecret   shared secret for the X-Signature check; null/blank means "no signature required"
 */
public record IngressConfig(int maxBodyBytes, String hmacSecret) {
    public static final String ENV_MAX_BODY_BYTES = "INGRESS_MAX_BODY_BYTES";
    public static final String ENV_HMAC_SECRET = "INGRESS_HMAC_SECRET";
    public static final int DEFAULT_MAX_BODY_BYTES = 262_144;

    /**
     * Reads settings from the environment.
     * On AWS the secret should come from SSM Parameter Store / Secrets Manager (fetched once at cold start
     * and cached); a plain environment variable is enough for now and for local runs.
     */
    public static IngressConfig fromEnv() {
        return from(System.getenv());
    }

    public static IngressConfig from(Map<String, String> env) {
        String max = env.get(ENV_MAX_BODY_BYTES);
        int maxBytes = (max == null || max.isBlank()) ? DEFAULT_MAX_BODY_BYTES : Integer.parseInt(max.trim());
        return new IngressConfig(maxBytes, env.get(ENV_HMAC_SECRET));
    }

    public boolean hmacEnabled() {
        return hmacSecret != null && !hmacSecret.isBlank();
    }
}
