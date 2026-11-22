package software.spool.infrastructure.provisioning;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.provisioning.ProvisioningProvider;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;

class ProvisioningProvidersTest {
    private static final Map<String, String> BUCKET = Map.of("bucket", "spool-lake", "region", "eu-west-1",
            "endpoint", "http://localhost:4570");

    @Test
    void available_hasAProviderForEveryTypeOfTheFramework() {
        assertThat(PluginResolver.available(ProvisioningProvider.class).keySet())
                .contains("S3", "KAFKA", "IN_MEMORY", "FILE_SYSTEM", "RAW_FILE_SYSTEM");
    }

    @Test
    void s3_locally_isABucketInALocalStackContainerOnThePortOfTheEndpoint() {
        TerraformFragment fragment = provision("S3", "lake", BUCKET, ProvisioningTarget.LOCAL);

        assertThat(fragment.resources().get("aws_s3_bucket").get("lake")).containsEntry("bucket", "spool-lake");
        assertThat(fragment.resources().get("docker_container")).containsKey("localstack");
        assertThat(fragment.resources().get("docker_container").get("localstack").get("ports"))
                .isEqualTo(List.of(Map.of("internal", 4566, "external", 4570)));
        assertThat(fragment.providers().get("aws")).containsEntry("endpoints", List.of(Map.of("s3", "http://localhost:4570")));
        assertThat(fragment.accesses()).isEmpty();
    }

    @Test
    void s3_inAws_isAnEncryptedBucketClosedToThePublicThatModulesReachObjectByObject() {
        TerraformFragment fragment = provision("S3", "lake", BUCKET, ProvisioningTarget.AWS);

        assertThat(fragment.resources()).containsOnlyKeys("aws_s3_bucket", "aws_s3_bucket_public_access_block",
                "aws_s3_bucket_server_side_encryption_configuration");
        assertThat(fragment.resources().get("aws_s3_bucket_public_access_block").get("lake"))
                .containsEntry("block_public_policy", true);
        assertThat(fragment.providers().get("aws")).isEqualTo(Map.of("region", "eu-west-1"));
        assertThat(fragment.accesses()).singleElement().satisfies(access -> {
            assertThat(access.resources()).containsExactly("${aws_s3_bucket.lake.arn}", "${aws_s3_bucket.lake.arn}/*");
            assertThat(access.actions()).contains("s3:GetObject", "s3:PutObject");
        });
    }

    @Test
    void kafka_locally_isABrokerInAContainerOnThePortOfItsFirstBootstrapServer() {
        TerraformFragment fragment = provision("KAFKA", "bus",
                Map.of("bootstrap.servers", "localhost:9192,localhost:9193"), ProvisioningTarget.LOCAL);

        Map<String, Object> container = fragment.resources().get("docker_container").get("kafka");
        assertThat(container.get("ports")).isEqualTo(List.of(Map.of("internal", 9092, "external", 9192)));
        assertThat(container.get("env")).asInstanceOf(LIST)
                .contains("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9192");
    }

    @Test
    void kafka_inAws_cannotBeGenerated() {
        assertThat(provisioning("KAFKA", "bus", Map.of("bootstrap.servers", "localhost:9092"))
                .forTarget(ProvisioningTarget.AWS)).isEmpty();
    }

    @Test
    void anInMemoryBus_needsNothingInAnyTarget() {
        PieceProvisioning provisioning = provisioning("IN_MEMORY", "bus", Map.of());

        assertThat(provisioning.forTarget(ProvisioningTarget.LOCAL)).contains(TerraformFragment.empty());
        assertThat(provisioning.forTarget(ProvisioningTarget.AWS)).contains(TerraformFragment.empty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM", "RAW_FILE_SYSTEM"})
    void aPieceOnTheDiskOfTheProcess_needsNothingLocallyAndCannotBeGeneratedForAws(String type) {
        PieceProvisioning provisioning = provisioning(type, "piece", Map.of("path", "/data"));

        assertThat(provisioning.forTarget(ProvisioningTarget.LOCAL)).contains(TerraformFragment.empty());
        assertThat(provisioning.forTarget(ProvisioningTarget.AWS)).isEmpty();
    }

    private static TerraformFragment provision(String type, String piece, Map<String, String> configuration,
                                               ProvisioningTarget target) {
        return provisioning(type, piece, configuration).forTarget(target).orElseThrow();
    }

    private static PieceProvisioning provisioning(String type, String piece, Map<String, String> configuration) {
        return PluginResolver.get(ProvisioningProvider.class, type)
                .create(PluginConfiguration.of(configuration, Map.of(ProvisioningProvider.PIECE, piece)));
    }
}
