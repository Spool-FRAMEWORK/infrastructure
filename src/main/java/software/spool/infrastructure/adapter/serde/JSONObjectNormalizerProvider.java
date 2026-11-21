package software.spool.infrastructure.adapter.serde;

import software.spool.crawler.api.normalizer.NormalizerPlan;
import software.spool.crawler.api.normalizer.StandardNormalizers;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.serde.ExtendableNormalizerProvider;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;

import java.util.stream.Stream;

/**
 * Turns a document that is one JSON object into one record, after adding the fields the rules take from the document.
 *
 * <p>It does not check that the document is an object: a JSON array is kept as a single record. Its stages are those
 * of {@link StandardNormalizers}.</p>
 */
@SpoolPlugin(NormalizerProvider.class)
public class JSONObjectNormalizerProvider implements ExtendableNormalizerProvider {
    @Override
    public String name() {
        return "JSON_OBJECT_NORMALIZER";
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
        return StandardNormalizers.jsonObject(EnrichmentRules.of(configuration), configuration.require("rootPath"));
    }
}
