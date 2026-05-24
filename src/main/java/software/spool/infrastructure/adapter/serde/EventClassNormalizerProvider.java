package software.spool.infrastructure.adapter.serde;

import software.spool.core.adapter.jackson.*;
import software.spool.core.pipeline.ObservedStep;
import software.spool.core.pipeline.Pipeline;
import software.spool.core.port.serde.EnrichmentRule;
import software.spool.crawler.internal.utils.factory.Normalizer;
import software.spool.crawler.internal.utils.factory.PayloadSplitterFactory;
import software.spool.crawler.internal.utils.factory.steps.LocateStep;
import software.spool.crawler.internal.utils.factory.steps.MapStep;
import software.spool.crawler.internal.utils.factory.steps.SerializeStep;
import software.spool.crawler.internal.utils.factory.steps.SplitEnrichStep;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;

import java.util.List;

@SpoolPlugin(NormalizerProvider.class)
public class EventClassNormalizerProvider implements NormalizerProvider {
    @Override
    public String name()  {
        return "EVENT_CLASS_NORMALIZER";
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
        List<EnrichmentRule> rules = rules(configuration);
        String rootPath = configuration.require("rootPath");

        return new Normalizer<>(Pipeline.<Object>start()
                .add(new ObservedStep<>("map-to-json", new MapStep<>(PayloadMapperFactory.jsonNode())))
                .add(new ObservedStep<>("locate", new LocateStep<>(PayloadLocatorFactory.fromRootPath(rootPath))))
                .add(new ObservedStep<>("split-enrich", new SplitEnrichStep<>(PayloadSplitterFactory.single(), PayloadExtractorFactory.withRules(rules), RecordEnricherFactory.json())))
                .add(new ObservedStep<>("serialize", new SerializeStep<>(RecordSerializerFactory.jsonNode()))));
    }

    private static List<EnrichmentRule> rules(PluginConfiguration configuration) {
        return PayloadDeserializerFactory.json().asList(EnrichmentRule.class)
                .deserialize(configuration.require("rules").getBytes());
    }
}