package software.spool.infrastructure.spi.provider.inbox;

import software.spool.core.port.idempotency.ProcessedKeyRepository;
import software.spool.infrastructure.spi.Plugin;

/**
 * Builds, from the configuration of an inbox piece, where the keys of the envelopes the janitor removes are kept
 * during the deduplication window (ADR-033).
 */
public interface ProcessedKeyRepositoryProvider extends Plugin<ProcessedKeyRepository> {
}
