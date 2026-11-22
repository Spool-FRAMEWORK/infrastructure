package software.spool.infrastructure.adapter.dataLake.postgresql;

import software.spool.infrastructure.provisioning.PieceProvisioning;
import software.spool.infrastructure.provisioning.ProvisioningTarget;
import software.spool.infrastructure.provisioning.TerraformFragment;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The Terraform of a PostgreSQL store of envelope headers: locally, a database in a container of its own, on the port
 * of its URL, ready before anything that depends on it starts. The adapter creates its table at startup, so only the
 * database is created here. In AWS it cannot be generated yet: RDS needs a network the generator knows nothing about.
 */
final class PostgreSqlProvisioning implements PieceProvisioning {
    static final String POSTGRES_IMAGE = "postgres:16-alpine";
    private static final int POSTGRES_PORT = 5432;
    private static final String DOCKER_SOURCE = "kreuzwerker/docker";
    private static final String DOCKER_VERSION = "~> 3.0";

    private final String piece;
    private final PostgreSqlAddress address;
    private final String user;
    private final String password;

    PostgreSqlProvisioning(String piece, PostgreSqlAddress address, String user, String password) {
        this.piece = piece;
        this.address = address;
        this.user = user;
        this.password = password;
    }

    @Override
    public Optional<TerraformFragment> forTarget(ProvisioningTarget target) {
        if (target != ProvisioningTarget.LOCAL) return Optional.empty();
        String containerVariable = piece + "_container_name";
        return Optional.of(TerraformFragment.builder()
                .requiredProvider("docker", DOCKER_SOURCE, DOCKER_VERSION)
                .variable("postgres_image", Map.of("type", "string", "default", POSTGRES_IMAGE))
                .variable(containerVariable, Map.of("type", "string", "default", "spool-" + piece))
                .resource("docker_image", "postgres", Map.of("name", "${var.postgres_image}", "keep_locally", true))
                .resource("docker_container", piece, Map.of(
                        "name", "${var." + containerVariable + "}",
                        "image", "${docker_image.postgres.image_id}",
                        "ports", List.of(Map.of("internal", POSTGRES_PORT, "external", address.port())),
                        "env", List.of(
                                "POSTGRES_DB=" + address.database(),
                                "POSTGRES_USER=" + user,
                                "POSTGRES_PASSWORD=" + password),
                        "healthcheck", List.of(Map.of(
                                "test", List.of("CMD-SHELL", "pg_isready -U " + user + " -d " + address.database()),
                                "interval", "2s",
                                "retries", 30)),
                        "wait", true))
                .build());
    }
}
