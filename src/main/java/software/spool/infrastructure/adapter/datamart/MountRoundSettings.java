package software.spool.infrastructure.adapter.datamart;

import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * How the rounds of the mounts on a data mart behave.
 *
 * @param saveEvery  how many objects an open round writes before it saves what it wrote, so a round whose process
 *                   stops is taken into account by the next one
 * @param turnLasts  how long the turn of a round lasts without being renewed, after which another round takes it
 * @param renewEvery how often an open round renews its turn, well below {@code turnLasts}
 * @param waitEvery  how often a round that waits for the turn of another looks again
 * @param elapsed    a monotonic count of nanoseconds of this process, which only measures how long a round waiting
 *                   for a turn has seen it unchanged, so it is never compared with the time of another process
 */
record MountRoundSettings(int saveEvery, Duration turnLasts, Duration renewEvery, Duration waitEvery, LongSupplier elapsed) {
    static final MountRoundSettings DEFAULT = new MountRoundSettings(1000, Duration.ofMinutes(10), Duration.ofMinutes(1),
            Duration.ofSeconds(1), System::nanoTime);

    MountRoundSettings withElapsed(LongSupplier elapsed) {
        return new MountRoundSettings(saveEvery, turnLasts, renewEvery, waitEvery, elapsed);
    }
}
