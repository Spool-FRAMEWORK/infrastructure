package software.spool.infrastructure.adapter.serde;

import software.spool.core.adapter.jackson.*;
import software.spool.crawler.internal.utils.factory.Normalizer;
import software.spool.crawler.internal.utils.factory.PayloadSplitterFactory;
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
    public boolean supports(PluginConfiguration configuration) {
        return true;
    }

    @Override
    public Normalizer<?, ?, ?> create(PluginConfiguration configuration) {
        return new Normalizer<>(
                PayloadDeserializerFactory.noOp(),
                PayloadExtractorFactory.noOp(),
                PayloadLocatorFactory.noOp(),
                PayloadSplitterFactory.single(),
                RecordEnricherFactory.noOp(),
                RecordSerializerFactory.noOp()
        );
    }
}
