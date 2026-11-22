package software.spool.infrastructure.adapter.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.spool.core.port.health.HealthCheck;
import software.spool.core.port.health.HealthProbe;

/**
 * Checks that the bucket of a piece answers and can be listed, asking for a single key of it.
 */
public final class S3HealthProbe implements HealthProbe {
    private static final String NAME = "S3";
    private static final int ONE_KEY = 1;

    private final S3Client s3Client;
    private final String bucket;

    public S3HealthProbe(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public HealthCheck probe() {
        try {
            s3Client.listObjectsV2(request -> request.bucket(bucket).maxKeys(ONE_KEY));
            return HealthCheck.healthy(NAME);
        } catch (RuntimeException e) {
            return HealthCheck.unhealthy(NAME, "The bucket " + bucket + " cannot be listed: " + e.getMessage());
        }
    }
}
