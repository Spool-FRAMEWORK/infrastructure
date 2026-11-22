package software.spool.infrastructure.spi.provider.health;

import software.spool.core.port.health.HealthProbe;
import software.spool.infrastructure.spi.Plugin;

/**
 * Builds the check of the pieces of one type: whether a piece answers, without changing what it keeps. Its name is
 * the {@code type} of the adapter it goes with, such as {@code S3}, and it receives the configuration of the piece.
 * A type without one is judged only by the calls its module makes to it.
 */
public interface HealthProbeProvider extends Plugin<HealthProbe> {
}
