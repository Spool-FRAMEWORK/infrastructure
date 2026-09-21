package software.spool.infrastructure.adapter.serde;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.port.serde.EnrichmentRule;
import software.spool.crawler.internal.utils.factory.Normalizer;
import software.spool.crawler.internal.utils.factory.NormalizerFactory;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;

import java.util.List;

/**
 * Turns a document that is one JSON object into one record, after adding the fields the rules take from the document.
 *
 * <p>It builds the same normalizer as {@code NormalizerFactory.jsonObject}. It does not check that the document is an
 * object: a JSON array is kept as a single record.</p>
 */
@SpoolPlugin(NormalizerProvider.class)
public class JSONObjectNormalizerProvider implements NormalizerProvider {
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
    public Normalizer<?> create(PluginConfiguration configuration) {
        return NormalizerFactory.jsonObject(rules(configuration), configuration.require("rootPath"));
    }

    private static List<EnrichmentRule> rules(PluginConfiguration configuration) {
        return PayloadDeserializerFactory.json().asList(EnrichmentRule.class)
                .deserialize(configuration.require("rules").getBytes());
    }
}
