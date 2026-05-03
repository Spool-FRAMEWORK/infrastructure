package software.spool.infrastructure.spi.provider;

import software.spool.crawler.api.port.source.PollSource;
import software.spool.infrastructure.spi.Plugin;

public interface PollSourceProvider extends Plugin<PollSource<?>> {
}
