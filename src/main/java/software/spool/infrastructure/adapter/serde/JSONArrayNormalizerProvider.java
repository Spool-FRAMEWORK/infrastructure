package software.spool.infrastructure.adapter.serde;

import software.spool.crawler.api.normalizer.NormalizerPlan;
import software.spool.crawler.api.normalizer.StandardNormalizers;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.serde.ExtendableNormalizerProvider;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;

import java.util.stream.Stream;

/** Turns a JSON array into one record per element, at the root path. Its stages are those of {@link StandardNormalizers}. */
@SpoolPlugin(NormalizerProvider.class)
public class JSONArrayNormalizerProvider implements ExtendableNormalizerProvider {
    @Override
    public String name() {
        return "JSON_ARRAY_NORMALIZER";
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
