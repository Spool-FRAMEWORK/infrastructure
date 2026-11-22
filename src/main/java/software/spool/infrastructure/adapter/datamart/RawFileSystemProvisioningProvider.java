package software.spool.infrastructure.adapter.datamart;

import software.spool.infrastructure.provisioning.ProcessDiskProvisioning;
import software.spool.infrastructure.provisioning.PieceProvisioning;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.provisioning.ProvisioningProvider;

/** The Terraform of a RAW_FILE_SYSTEM data mart: nothing locally, and it cannot be created in AWS. */
@SpoolPlugin(ProvisioningProvider.class)
public class RawFileSystemProvisioningProvider implements ProvisioningProvider {
    @Override
    public String name() {
        return "RAW_FILE_SYSTEM";
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
    public PieceProvisioning create(PluginConfiguration configuration) {
        return ProcessDiskProvisioning.INSTANCE;
    }
}
