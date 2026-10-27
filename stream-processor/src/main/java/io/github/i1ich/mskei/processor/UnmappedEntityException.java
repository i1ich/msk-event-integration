package io.github.i1ich.mskei.processor;

/** The Salesforce event is valid, but we have no canonical mapping for its entity (only Account is mapped). */
public class UnmappedEntityException extends RuntimeException {
    public UnmappedEntityException(String entityName) {
        super("No mapping for entity '" + entityName + "'");
    }
}
