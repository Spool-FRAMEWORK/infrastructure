package software.spool.infrastructure.adapter.s3;

import software.spool.infrastructure.provisioning.PieceProvisioning;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.provisioning.ProvisioningProvider;

/** The Terraform of every S3 piece, inbox or data lake alike: the bucket of its configuration. */
@SpoolPlugin(ProvisioningProvider.class)
public class S3ProvisioningProvider implements ProvisioningProvider {
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
    public PieceProvisioning create(PluginConfiguration configuration) {
        return new S3Provisioning(configuration.require(PIECE, String.class), configuration.require("bucket"),
                configuration.require("region"), configuration.require("endpoint"));
    }
}
