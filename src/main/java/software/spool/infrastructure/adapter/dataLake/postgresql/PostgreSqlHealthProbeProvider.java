package software.spool.infrastructure.adapter.dataLake.postgresql;

import software.spool.core.port.health.HealthProbe;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.health.HealthProbeProvider;

@SpoolPlugin(HealthProbeProvider.class)
public class PostgreSqlHealthProbeProvider implements HealthProbeProvider {
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
    public HealthProbe create(PluginConfiguration configuration) {
        return PostgreSqlEnvelopeHeaderStores.healthProbeOf(configuration);
    }
}
