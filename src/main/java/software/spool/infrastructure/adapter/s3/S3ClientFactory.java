package software.spool.infrastructure.adapter.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.net.URI;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * Builds the {@link S3Client} that every S3 provider uses, so the region, the endpoint and the credentials are
 * read in one place.
 *
 * <p>The descriptor never holds a secret. It names the environment variables that do, with
 * {@value #ACCESS_KEY_ENV} and {@value #SECRET_KEY_ENV}. Both must be given together, and each variable must
 * be set. Without either key the default AWS provider chain is used: environment, profile, instance role and
 * so on. Anything in between is rejected when the descriptor is loaded, saying what is missing.</p>
 *
 * <p>An endpoint is used for S3-compatible stores such as MinIO or R2, which need path style access.</p>
 */
public final class S3ClientFactory {

    public static final String ACCESS_KEY_ENV = "accessKeyEnv";
    public static final String SECRET_KEY_ENV = "secretKeyEnv";

    private S3ClientFactory() {}

    /**
     * Builds the client described by the configuration, reading credentials from the process environment.
     *
     * @param configuration holds {@code region}, {@code endpoint} and optionally the two credential keys
     * @return the client
     * @throws IllegalArgumentException if a required key is missing or the credentials are incomplete
     */
    public static S3Client create(PluginConfiguration configuration) {
        return create(configuration, System::getenv);
    }

    /**
     * Same as {@link #create(PluginConfiguration)}, reading the environment through the given function.
     *
     * @param configuration holds {@code region}, {@code endpoint} and optionally the two credential keys
     * @param environment   returns the value of an environment variable, or {@code null} when it is not set
     * @return the client
     * @throws IllegalArgumentException if a required key is missing or the credentials are incomplete
     */
    public static S3Client create(PluginConfiguration configuration, UnaryOperator<String> environment) {
        String endpoint = configuration.require("endpoint");
        var builder = S3Client.builder()
                .region(Region.of(configuration.require("region")))
                .credentialsProvider(credentials(configuration, environment));
        if (!endpoint.isBlank()) {
            builder.endpointOverride(URI.create(endpoint))
                    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
        }
        return builder.build();
    }

    static AwsCredentialsProvider credentials(PluginConfiguration configuration, UnaryOperator<String> environment) {
        Optional<String> accessKeyName = named(configuration, ACCESS_KEY_ENV);
        Optional<String> secretKeyName = named(configuration, SECRET_KEY_ENV);
        if (accessKeyName.isEmpty() && secretKeyName.isEmpty()) return DefaultCredentialsProvider.create();
        if (accessKeyName.isEmpty()) throw incomplete(ACCESS_KEY_ENV);
        if (secretKeyName.isEmpty()) throw incomplete(SECRET_KEY_ENV);
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(
                valueOf(accessKeyName.get(), ACCESS_KEY_ENV, environment),
                valueOf(secretKeyName.get(), SECRET_KEY_ENV, environment)));
    }

    private static Optional<String> named(PluginConfiguration configuration, String key) {
        return configuration.get(key).filter(name -> !name.isBlank());
    }

    private static String valueOf(String variable, String key, UnaryOperator<String> environment) {
        String value = environment.apply(variable);
        if (value == null || value.isBlank())
            throw new IllegalArgumentException(
                    "The environment variable '" + variable + "' named by " + key + " is not set");
        return value;
    }

    private static IllegalArgumentException incomplete(String missing) {
        return new IllegalArgumentException(
                "S3 credentials need both " + ACCESS_KEY_ENV + " and " + SECRET_KEY_ENV + ", but " + missing + " is missing");
    }
}
