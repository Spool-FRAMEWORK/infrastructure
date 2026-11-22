package software.spool.infrastructure.adapter.dataLake.postgresql;

import software.spool.core.port.datalake.EnvelopeHeaderWriter;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.EnvelopeHeaderWriterProvider;

@SpoolPlugin(EnvelopeHeaderWriterProvider.class)
public class PostgreSqlEnvelopeHeaderWriterProvider implements EnvelopeHeaderWriterProvider {
    @Override
    public String name() {
        return PostgreSqlEnvelopeHeaderStores.TYPE;
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has(PostgreSqlEnvelopeHeaderStores.URL);
    }

    @Override
    public EnvelopeHeaderWriter create(PluginConfiguration configuration) {
        return PostgreSqlEnvelopeHeaderStores.storeOf(configuration);
    }
}
