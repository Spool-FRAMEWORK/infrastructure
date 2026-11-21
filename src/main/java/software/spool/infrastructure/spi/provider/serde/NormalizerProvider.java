package software.spool.infrastructure.spi.provider.serde;

import software.spool.crawler.api.normalizer.Normalizer;
import software.spool.infrastructure.spi.Plugin;

public interface NormalizerProvider extends Plugin<Normalizer<?>> {
}
