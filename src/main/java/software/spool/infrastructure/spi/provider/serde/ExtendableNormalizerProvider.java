package software.spool.infrastructure.spi.provider.serde;

import software.spool.crawler.api.normalizer.Normalizer;
import software.spool.crawler.api.normalizer.NormalizerPlan;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.stream.Stream;

/**
 * A normalizer provider that publishes its plan, so that a descriptor can add or replace steps at its stages
 * instead of copying it whole.
 */
public interface ExtendableNormalizerProvider extends NormalizerProvider {

    NormalizerPlan<?, Stream<byte[]>> plan(PluginConfiguration configuration);

    @Override
    default Normalizer<?> create(PluginConfiguration configuration) {
        return plan(configuration).build();
    }
}
