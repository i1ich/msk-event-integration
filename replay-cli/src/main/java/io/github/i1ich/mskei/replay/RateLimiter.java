package io.github.i1ich.mskei.replay;

import java.util.function.LongSupplier;

/**
 * Fixed-interval rate limiter: at most one send per {@code 1s / rate}.
 * Clock and sleeper are injected so tests do not really sleep.
 * No catch-up: if we were slow, the next send is not "faster" to make up for it (a burst could overload downstream).
 */
public final class RateLimiter {

    /** Sleeps for the given number of nanoseconds. */
    @FunctionalInterface
    public interface Sleeper {
        void sleepNanos(long nanos) throws InterruptedException;
    }

    private final long intervalNanos;
    private final LongSupplier clockNanos;
    private final Sleeper sleeper;
    private boolean started = false;
    private long nextAllowedAt;

    public RateLimiter(double permitsPerSecond, LongSupplier clockNanos, Sleeper sleeper) {
        if (!(permitsPerSecond > 0)) {
            throw new IllegalArgumentException("rate must be > 0, got " + permitsPerSecond);
        }
        this.intervalNanos = (long) (1_000_000_000d / permitsPerSecond);
        this.clockNanos = clockNanos;
        this.sleeper = sleeper;
    }

    /** Production limiter: real clock, real sleep. */
    public static RateLimiter system(double permitsPerSecond) {
        return new RateLimiter(permitsPerSecond, System::nanoTime,
                nanos -> Thread.sleep(nanos / 1_000_000, (int) (nanos % 1_000_000)));
    }

    public long intervalNanos() {
        return intervalNanos;
    }

    /** Blocks until the caller may send the next message. The first call never waits. */
    public void acquire() throws InterruptedException {
        long now = clockNanos.getAsLong();
        if (started && now < nextAllowedAt) {
            sleeper.sleepNanos(nextAllowedAt - now);
            now = nextAllowedAt;
        }
        started = true;
        nextAllowedAt = now + intervalNanos;
    }
}
