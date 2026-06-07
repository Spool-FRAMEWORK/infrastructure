package software.spool.infrastructure.spi.provider;

import software.spool.crawler.api.port.source.StreamSource;
import software.spool.infrastructure.spi.Plugin;

public interface StreamSourceProvider extends Plugin<StreamSource<?>> {
}
