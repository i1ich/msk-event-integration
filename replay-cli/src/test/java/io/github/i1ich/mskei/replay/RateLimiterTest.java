package io.github.i1ich.mskei.replay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class RateLimiterTest {
    private long now = 0;
    private final List<Long> sleeps = new ArrayList<>();

    /** Fake sleeper moves the fake clock forward instead of really sleeping. */
    private RateLimiter limiter(double rate) {
        return new RateLimiter(rate, () -> now, nanos -> {
            sleeps.add(nanos);
            now += nanos;
        });
    }

    @Test
    void intervalIsOneSecondDividedByRate() {
        assertThat(limiter(10).intervalNanos()).isEqualTo(100_000_000L);
        assertThat(limiter(4).intervalNanos()).isEqualTo(250_000_000L);
    }

    @Test
    void firstCallDoesNotWaitThenEachCallWaitsOneInterval() throws Exception {
        RateLimiter l = limiter(10);
        l.acquire();
        l.acquire();
        l.acquire();
        assertThat(sleeps).containsExactly(100_000_000L, 100_000_000L);
    }

    @Test
    void onlyWaitsForTheRemainingTime() throws Exception {
        RateLimiter l = limiter(10);
        l.acquire();
        now += 30_000_000L; // caller did other work for 30 ms
        l.acquire();
        assertThat(sleeps).containsExactly(70_000_000L);
    }

    @Test
    void doesNotCatchUpAfterBeingSlow() throws Exception {
        RateLimiter l = limiter(10);
        l.acquire();
        now += 500_000_000L; // way over the interval
        l.acquire();
        l.acquire(); // must wait a full interval again: no burst
        assertThat(sleeps).containsExactly(100_000_000L);
    }

    @Test
    void rejectsNonPositiveRate() {
        assertThatThrownBy(() -> limiter(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> limiter(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> limiter(Double.NaN)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void systemLimiterWorks() throws Exception {
        RateLimiter l = RateLimiter.system(1000);
        l.acquire();
        l.acquire();
        assertThat(l.intervalNanos()).isEqualTo(1_000_000L);
    }
}
