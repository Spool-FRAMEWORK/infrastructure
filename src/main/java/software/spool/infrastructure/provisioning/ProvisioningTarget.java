package software.spool.infrastructure.provisioning;

import java.util.Locale;

/** Where the generated Terraform creates the pieces: on the machine that applies it, or in AWS. */
public enum ProvisioningTarget {
    LOCAL,
    AWS;

    /** The name a person writes, such as {@code aws}. */
    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }
}
