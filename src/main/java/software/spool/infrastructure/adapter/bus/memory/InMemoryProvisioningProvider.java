package software.spool.infrastructure.adapter.bus.memory;

import software.spool.infrastructure.provisioning.PieceProvisioning;
import software.spool.infrastructure.provisioning.TerraformFragment;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.provisioning.ProvisioningProvider;

import java.util.Optional;

/** The Terraform of an in-memory bus: nothing, in any target, since it lives in the process wherever it runs. */
@SpoolPlugin(ProvisioningProvider.class)
public class InMemoryProvisioningProvider implements ProvisioningProvider {
    @Override
    public String name() {
        return "IN_MEMORY";
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
        return target -> Optional.of(TerraformFragment.empty());
    }
}
