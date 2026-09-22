package software.spool.infrastructure.spi.provider.quarantine;

import software.spool.infrastructure.spi.Plugin;
import software.spool.ingester.api.port.QuarantineStore;

public interface QuarantineStoreProvider extends Plugin<QuarantineStore> {
}
