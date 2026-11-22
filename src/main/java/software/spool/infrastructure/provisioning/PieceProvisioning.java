package software.spool.infrastructure.provisioning;

import java.util.Optional;

/** The Terraform of one piece. Nothing when its type cannot be generated for the target. */
@FunctionalInterface
public interface PieceProvisioning {
    Optional<TerraformFragment> forTarget(ProvisioningTarget target);
}
