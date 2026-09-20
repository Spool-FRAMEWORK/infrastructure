package software.spool.infrastructure.adapter.s3;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.net.URI;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3ClientFactoryTest {

    private static final UnaryOperator<String> ENVIRONMENT =
            Map.of("R2_ACCESS_KEY", "the-access-key", "R2_SECRET_KEY", "the-secret-key", "EMPTY", "  ")::get;

    private static PluginConfiguration configuration(String... keysAndValues) {
        PluginConfiguration.Builder builder = PluginConfiguration.builder()
                .with("region", "auto")
                .with("bucket", "spool")
                .with("endpoint", "http://localhost:9000");
        for (int i = 0; i < keysAndValues.length; i += 2) builder.with(keysAndValues[i], keysAndValues[i + 1]);
        return builder.build();
    }

    @Test
    void withoutCredentialKeysTheDefaultProviderChainIsUsed() {
        AwsCredentialsProvider provider = S3ClientFactory.credentials(configuration(), ENVIRONMENT);

        assertThat(provider).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void blankCredentialKeysAreTheSameAsNoKeys() {
        AwsCredentialsProvider provider = S3ClientFactory.credentials(
                configuration("accessKeyEnv", " ", "secretKeyEnv", ""), ENVIRONMENT);

        assertThat(provider).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void bothKeysReadTheCredentialsFromTheNamedEnvironmentVariables() {
        AwsCredentials credentials = S3ClientFactory.credentials(
                configuration("accessKeyEnv", "R2_ACCESS_KEY", "secretKeyEnv", "R2_SECRET_KEY"), ENVIRONMENT)
                .resolveCredentials();

        assertThat(credentials.accessKeyId()).isEqualTo("the-access-key");
        assertThat(credentials.secretAccessKey()).isEqualTo("the-secret-key");
    }

    @Test
    void theDescriptorHoldsTheNameOfTheVariableAndNeverTheValue() {
        AwsCredentials credentials = S3ClientFactory.credentials(
                configuration("accessKeyEnv", "R2_ACCESS_KEY", "secretKeyEnv", "R2_SECRET_KEY"), ENVIRONMENT)
                .resolveCredentials();

        assertThat(credentials.accessKeyId()).isNotEqualTo("R2_ACCESS_KEY");
        assertThat(credentials.secretAccessKey()).isNotEqualTo("R2_SECRET_KEY");
    }

    @Test
    void onlyTheAccessKeyIsRejectedSayingWhichOneIsMissing() {
        assertThatThrownBy(() -> S3ClientFactory.credentials(configuration("accessKeyEnv", "R2_ACCESS_KEY"), ENVIRONMENT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("S3 credentials need both accessKeyEnv and secretKeyEnv, but secretKeyEnv is missing");
    }

    @Test
    void onlyTheSecretKeyIsRejectedSayingWhichOneIsMissing() {
        assertThatThrownBy(() -> S3ClientFactory.credentials(configuration("secretKeyEnv", "R2_SECRET_KEY"), ENVIRONMENT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("S3 credentials need both accessKeyEnv and secretKeyEnv, but accessKeyEnv is missing");
    }

    @Test
    void aVariableThatIsNotSetIsRejectedNamingIt() {
        assertThatThrownBy(() -> S3ClientFactory.credentials(
                configuration("accessKeyEnv", "NOT_SET", "secretKeyEnv", "R2_SECRET_KEY"), ENVIRONMENT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The environment variable 'NOT_SET' named by accessKeyEnv is not set");
    }

    @Test
    void aVariableThatIsBlankIsRejectedLikeOneThatIsNotSet() {
        assertThatThrownBy(() -> S3ClientFactory.credentials(
                configuration("accessKeyEnv", "R2_ACCESS_KEY", "secretKeyEnv", "EMPTY"), ENVIRONMENT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The environment variable 'EMPTY' named by secretKeyEnv is not set");
    }

    @Test
    void theClientPointsAtTheEndpointOfTheDescriptor() {
        S3Client client = S3ClientFactory.create(
                configuration("accessKeyEnv", "R2_ACCESS_KEY", "secretKeyEnv", "R2_SECRET_KEY"), ENVIRONMENT);

        assertThat(client.serviceClientConfiguration().endpointOverride()).contains(URI.create("http://localhost:9000"));
        assertThat(client.serviceClientConfiguration().region().id()).isEqualTo("auto");
    }

    @Test
    void anIncompleteCredentialFailsWhenTheClientIsCreated() {
        assertThatThrownBy(() -> S3ClientFactory.create(configuration("accessKeyEnv", "R2_ACCESS_KEY"), ENVIRONMENT))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
