package software.spool.infrastructure.adapter.s3;

import software.spool.infrastructure.provisioning.PieceProvisioning;
import software.spool.infrastructure.provisioning.ProvisioningTarget;
import software.spool.infrastructure.provisioning.ResourceAccess;
import software.spool.infrastructure.provisioning.TerraformFragment;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The Terraform of a bucket. Locally the bucket lives in a LocalStack container that every S3 piece shares, reached at
 * the endpoint of the piece; in AWS it is a real bucket, encrypted and closed to the public.
 */
final class S3Provisioning implements PieceProvisioning {
    static final String LOCALSTACK_IMAGE = "localstack/localstack:3.8";
    static final String LOCALSTACK_CONTAINER = "spool-localstack";
    private static final int LOCALSTACK_PORT = 4566;
    private static final String AWS_SOURCE = "hashicorp/aws";
    private static final String AWS_VERSION = "~> 5.0";
    private static final String DOCKER_SOURCE = "kreuzwerker/docker";
    private static final String DOCKER_VERSION = "~> 3.0";
    private static final List<String> OBJECT_ACTIONS = List.of("s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket");

    private final String piece;
    private final String bucket;
    private final String region;
    private final String endpoint;

    S3Provisioning(String piece, String bucket, String region, String endpoint) {
        this.piece = piece;
        this.bucket = bucket;
        this.region = region;
        this.endpoint = endpoint;
    }

    @Override
    public Optional<TerraformFragment> forTarget(ProvisioningTarget target) {
        return Optional.of(target == ProvisioningTarget.LOCAL ? local() : aws());
    }

    private TerraformFragment local() {
        return TerraformFragment.builder()
                .requiredProvider("aws", AWS_SOURCE, AWS_VERSION)
                .requiredProvider("docker", DOCKER_SOURCE, DOCKER_VERSION)
                .variable("localstack_image", Map.of("type", "string", "default", LOCALSTACK_IMAGE))
                .variable("localstack_container_name", Map.of("type", "string", "default", LOCALSTACK_CONTAINER))
                .resource("docker_image", "localstack", Map.of("name", "${var.localstack_image}", "keep_locally", true))
                .resource("docker_container", "localstack", Map.of(
                        "name", "${var.localstack_container_name}",
                        "image", "${docker_image.localstack.image_id}",
                        "ports", List.of(Map.of("internal", LOCALSTACK_PORT, "external", portOf(endpoint))),
                        "healthcheck", List.of(Map.of(
                                "test", List.of("CMD", "curl", "-f", "http://localhost:" + LOCALSTACK_PORT + "/_localstack/health"),
                                "interval", "5s",
                                "retries", 30)),
                        "wait", true))
                .provider("aws", Map.of(
                        "region", region,
                        "access_key", "test",
                        "secret_key", "test",
                        "skip_credentials_validation", true,
                        "skip_metadata_api_check", true,
                        "skip_requesting_account_id", true,
                        "s3_use_path_style", true,
                        "endpoints", List.of(Map.of("s3", endpoint))))
                .resource("aws_s3_bucket", piece, Map.of(
                        "bucket", bucket,
                        "force_destroy", true,
                        "depends_on", List.of("docker_container.localstack")))
                .build();
    }

    private TerraformFragment aws() {
        String bucketId = "${aws_s3_bucket." + piece + ".id}";
        String bucketArn = "${aws_s3_bucket." + piece + ".arn}";
        return TerraformFragment.builder()
                .requiredProvider("aws", AWS_SOURCE, AWS_VERSION)
                .provider("aws", Map.of("region", region))
                .resource("aws_s3_bucket", piece, Map.of("bucket", bucket))
                .resource("aws_s3_bucket_public_access_block", piece, Map.of(
                        "bucket", bucketId,
                        "block_public_acls", true,
                        "block_public_policy", true,
                        "ignore_public_acls", true,
                        "restrict_public_buckets", true))
                .resource("aws_s3_bucket_server_side_encryption_configuration", piece, Map.of(
                        "bucket", bucketId,
                        "rule", List.of(Map.of("apply_server_side_encryption_by_default",
                                List.of(Map.of("sse_algorithm", "AES256"))))))
                .access(new ResourceAccess(List.of(bucketArn, bucketArn + "/*"), OBJECT_ACTIONS))
                .build();
    }

    /** The port of the endpoint, or LocalStack's own when the endpoint does not say it or is a variable. */
    private static int portOf(String endpoint) {
        try {
            int port = URI.create(endpoint).getPort();
            return port > 0 ? port : LOCALSTACK_PORT;
        } catch (IllegalArgumentException notAnAddress) {
            return LOCALSTACK_PORT;
        }
    }
}
