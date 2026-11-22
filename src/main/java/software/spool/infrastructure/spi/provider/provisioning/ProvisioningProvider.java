package software.spool.infrastructure.spi.provider.provisioning;

import software.spool.infrastructure.provisioning.PieceProvisioning;
import software.spool.infrastructure.spi.Plugin;

/**
 * Generates the Terraform of the pieces of one type. Its name is the {@code type} of the adapter it goes with, such as
 * {@code S3}, and it receives the configuration of the piece and its name in the context key {@value #PIECE}.
 */
public interface ProvisioningProvider extends Plugin<PieceProvisioning> {
    String PIECE = "piece";
}
