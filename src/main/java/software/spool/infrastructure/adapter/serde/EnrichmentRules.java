package software.spool.infrastructure.adapter.serde;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.port.serde.EnrichmentRule;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.List;

/** The enrichment rules a normalizer is configured with, which arrive as JSON under {@code rules}. */
final class EnrichmentRules {

    private EnrichmentRules() {}

    static List<EnrichmentRule> of(PluginConfiguration configuration) {
        return PayloadDeserializerFactory.json().asList(EnrichmentRule.class)
                .deserialize(configuration.require("rules").getBytes());
    }
}
