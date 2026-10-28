package io.github.i1ich.mskei.ingress;

/** Reads a secret string by id/ARN. A tiny interface so tests do not need AWS. */
@FunctionalInterface
public interface SecretReader {
    String read(String secretId);
}
