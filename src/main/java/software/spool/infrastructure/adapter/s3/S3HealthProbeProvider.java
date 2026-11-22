package software.spool.infrastructure.adapter.s3;

import software.spool.core.port.health.HealthProbe;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.health.HealthProbeProvider;

@SpoolPlugin(HealthProbeProvider.class)
public class S3HealthProbeProvider implements HealthProbeProvider {
    @Override
    public String name() {
        return "S3";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("bucket") && configuration.has("region") && configuration.has("endpoint");
    }

    @Override
    public HealthProbe create(PluginConfiguration configuration) {
        return new S3HealthProbe(S3ClientFactory.create(configuration), configuration.require("bucket"));
    }
}
