package software.spool.infrastructure.adapter.serde;

import software.spool.crawler.api.normalizer.NormalizerPlan;
import software.spool.crawler.api.normalizer.StandardNormalizers;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.serde.ExtendableNormalizerProvider;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;

import java.util.stream.Stream;

/** The same normalizer as {@link JSONArrayNormalizerProvider}, under the media type {@code JSON}. */
@SpoolPlugin(NormalizerProvider.class)
public class JSONNormalizerProvider implements ExtendableNormalizerProvider {
    @Override
    public String name() {
        return "JSON_NORMALIZER";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("rules") && configuration.has("rootPath");
    }

    @Override
    public NormalizerPlan<byte[], Stream<byte[]>> plan(PluginConfiguration configuration) {
        return StandardNormalizers.jsonArray(EnrichmentRules.of(configuration), configuration.require("rootPath"));
    }
}
