package io.github.i1ich.mskei.it;

import io.github.i1ich.mskei.it.support.ItEnvironment;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class: touching {@link ItEnvironment#get()} starts the shared containers and in-process services on first
 * use; later test classes in the same JVM reuse them.
 */
abstract class AbstractIT {
    protected static final ItEnvironment ENV = ItEnvironment.get();

    @BeforeEach
    void resetDownstream() {
        // Back to "Started" for the 503-once scenarios; request log cleared. Stubs from infra/wiremock stay.
        ENV.wireMock().resetScenarios();
        ENV.wireMock().resetRequests();
    }
}
