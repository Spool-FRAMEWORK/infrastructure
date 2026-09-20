package software.spool.infrastructure.adapter.s3;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.spool.infrastructure.adapter.dataLake.s3.S3DataLakeWriterProvider;
import software.spool.infrastructure.adapter.inbox.s3.S3InboxEnvelopeRemoverProvider;
import software.spool.infrastructure.adapter.inbox.s3.S3InboxReaderProvider;
import software.spool.infrastructure.adapter.inbox.s3.S3InboxUpdaterProvider;
import software.spool.infrastructure.adapter.inbox.s3.S3InboxWriterProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3ProvidersCredentialsTest {

    static Stream<Named<Function<PluginConfiguration, Object>>> providers() {
        return Stream.of(
                Named.of("S3InboxWriterProvider", configuration -> new S3InboxWriterProvider().create(configuration)),
                Named.of("S3InboxReaderProvider", configuration -> new S3InboxReaderProvider().create(configuration)),
                Named.of("S3InboxUpdaterProvider", configuration -> new S3InboxUpdaterProvider().create(configuration)),
                Named.of("S3InboxEnvelopeRemoverProvider", configuration -> new S3InboxEnvelopeRemoverProvider().create(configuration)),
                Named.of("S3DataLakeWriterProvider", configuration -> new S3DataLakeWriterProvider().create(configuration)));
    }

    private static PluginConfiguration.Builder configuration() {
        return PluginConfiguration.builder()
                .with("region", "auto")
                .with("bucket", "spool")
                .with("endpoint", "http://localhost:9000");
    }

    @ParameterizedTest
    @MethodSource("providers")
    void aProviderWithoutCredentialKeysCreatesItsComponent(Function<PluginConfiguration, Object> create) {
        assertThat(create.apply(configuration().build())).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("providers")
    void aProviderRejectsAnIncompleteCredential(Function<PluginConfiguration, Object> create) {
        PluginConfiguration incomplete = configuration().with("accessKeyEnv", "SPOOL_TEST_ACCESS_KEY").build();

        assertThatThrownBy(() -> create.apply(incomplete))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("S3 credentials need both accessKeyEnv and secretKeyEnv, but secretKeyEnv is missing");
    }

    @ParameterizedTest
    @MethodSource("providers")
    void aProviderRejectsAVariableThatIsNotSet(Function<PluginConfiguration, Object> create) {
        PluginConfiguration unset = configuration()
                .with("accessKeyEnv", "SPOOL_TEST_VARIABLE_THAT_DOES_NOT_EXIST")
                .with("secretKeyEnv", "SPOOL_TEST_VARIABLE_THAT_DOES_NOT_EXIST_EITHER")
                .build();

        assertThatThrownBy(() -> create.apply(unset))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The environment variable 'SPOOL_TEST_VARIABLE_THAT_DOES_NOT_EXIST' named by accessKeyEnv is not set");
    }
}
