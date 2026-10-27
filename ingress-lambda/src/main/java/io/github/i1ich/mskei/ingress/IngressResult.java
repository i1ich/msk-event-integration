package io.github.i1ich.mskei.ingress;

/** HTTP-agnostic outcome of handling one request: a status code and a JSON body. */
public record IngressResult(int status, String body) {
}
