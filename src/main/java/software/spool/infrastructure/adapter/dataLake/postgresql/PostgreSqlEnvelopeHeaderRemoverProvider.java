package software.spool.infrastructure.adapter.dataLake.postgresql;

import software.spool.core.port.datalake.EnvelopeHeaderRemover;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.EnvelopeHeaderRemoverProvider;

@SpoolPlugin(EnvelopeHeaderRemoverProvider.class)
public class PostgreSqlEnvelopeHeaderRemoverProvider implements EnvelopeHeaderRemoverProvider {
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
    public EnvelopeHeaderRemover create(PluginConfiguration configuration) {
        return PostgreSqlEnvelopeHeaderStores.of(configuration);
    }
}
