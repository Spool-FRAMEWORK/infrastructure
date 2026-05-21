package software.spool.infrastructure.adapter.serde;

import com.fasterxml.jackson.databind.JsonNode;
import software.spool.core.adapter.jackson.*;
import software.spool.core.pipeline.ObservedStep;
import software.spool.core.pipeline.Pipeline;
import software.spool.core.port.serde.EnrichmentRule;
import software.spool.crawler.internal.utils.factory.Normalizer;
import software.spool.crawler.internal.utils.factory.PayloadSplitterFactory;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;

@SpoolPlugin(NormalizerProvider.class)
public class JSONNormalizerProvider implements NormalizerProvider {
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
    public Normalizer<?, ?, ?> create(PluginConfiguration configuration) {
        return new Normalizer<>(
                PayloadDeserializerFactory.json().asNode(),
                PayloadExtractorFactory.withRules(PayloadDeserializerFactory.json().asList(EnrichmentRule.class)
                                .deserialize(configuration.require("rules").getBytes())),
                PayloadLocatorFactory.fromRootPath(configuration.require("rootPath")),
                PayloadSplitterFactory.single(),
                RecordEnricherFactory.json(),
                RecordSerializerFactory.jsonNode());
    }
}
