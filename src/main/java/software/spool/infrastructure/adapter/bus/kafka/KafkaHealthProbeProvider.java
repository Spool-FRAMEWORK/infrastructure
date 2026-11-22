package software.spool.infrastructure.adapter.bus.kafka;

import software.spool.core.port.health.HealthProbe;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.health.HealthProbeProvider;

@SpoolPlugin(HealthProbeProvider.class)
public class KafkaHealthProbeProvider implements HealthProbeProvider {
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    @Override
    public String name() {
        return "KAFKA";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has(BOOTSTRAP_SERVERS);
    }

    @Override
    public HealthProbe create(PluginConfiguration configuration) {
        return new KafkaHealthProbe(configuration.require(BOOTSTRAP_SERVERS), KafkaHealthProbe.DEFAULT_TIMEOUT);
    }
}
