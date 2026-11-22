package software.spool.infrastructure.adapter.bus.kafka;

import software.spool.infrastructure.provisioning.PieceProvisioning;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.provisioning.ProvisioningProvider;

@SpoolPlugin(ProvisioningProvider.class)
public class KafkaProvisioningProvider implements ProvisioningProvider {
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
        return configuration.has("bootstrap.servers");
    }

    @Override
    public PieceProvisioning create(PluginConfiguration configuration) {
        return new KafkaProvisioning(configuration.require("bootstrap.servers"));
    }
}
