package software.spool.infrastructure.provisioning;

import java.util.Optional;

/**
 * A piece that lives on the disk of the process: there is nothing to create for it locally, and it cannot be
 * created in AWS.
 */
public final class ProcessDiskProvisioning implements PieceProvisioning {
    public static final ProcessDiskProvisioning INSTANCE = new ProcessDiskProvisioning();

    private ProcessDiskProvisioning() {}

    @Override
    public Optional<TerraformFragment> forTarget(ProvisioningTarget target) {
        return target == ProvisioningTarget.LOCAL ? Optional.of(TerraformFragment.empty()) : Optional.empty();
    }
}
