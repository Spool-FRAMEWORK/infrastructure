package software.spool.infrastructure.adapter.serde;

import software.spool.core.adapter.jackson.*;
import software.spool.core.pipeline.ObservedStep;
import software.spool.core.pipeline.Pipeline;
import software.spool.crawler.internal.utils.factory.Normalizer;
import software.spool.crawler.internal.utils.factory.PayloadSplitterFactory;
import software.spool.crawler.internal.utils.factory.steps.SerializeStep;
import software.spool.crawler.internal.utils.factory.steps.SplitEnrichStep;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;

@SpoolPlugin(NormalizerProvider.class)
public class PDFNormalizerProvider implements NormalizerProvider {
    @Override
    public String name() {
        return "PDF_NORMALIZER";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) { return true; }

    @Override
    public Normalizer<?> create(PluginConfiguration configuration) {
        return new Normalizer<>(Pipeline.<byte[]>start()
                .add(new ObservedStep<>("split-enrich", new SplitEnrichStep<>(PayloadSplitterFactory.single(), PayloadExtractorFactory.noOp(), RecordEnricherFactory.noOp())))
                .add(new ObservedStep<>("serialize", new SerializeStep<>(RecordSerializerFactory.noOp()))));
    }
}