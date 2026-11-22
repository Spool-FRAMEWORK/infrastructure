package software.spool.infrastructure.spi.provider.dataLake;

import software.spool.infrastructure.spi.Plugin;
import software.spool.mounter.api.port.scaling.PartitionDiscovery;

/**
 * Builds the {@link PartitionDiscovery} of a data lake. Its configuration holds, besides the properties of the piece,
 * the {@link software.spool.mounter.api.MountMode} of the mount under {@value #MODE}, which says the layer it lists.
 */
public interface PartitionDiscoveryProvider extends Plugin<PartitionDiscovery> {
    String MODE = "mode";
}
