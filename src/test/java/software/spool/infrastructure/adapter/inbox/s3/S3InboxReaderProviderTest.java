package software.spool.infrastructure.adapter.inbox.s3;

import org.junit.jupiter.api.Test;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3InboxReaderProviderTest {

    private final S3InboxReaderProvider provider = new S3InboxReaderProvider();

    private static PluginConfiguration.Builder configuration() {
        return PluginConfiguration.builder()
                .with("region", "auto")
                .with("bucket", "spool")
                .with("endpoint", "http://localhost:9000");
    }

    @Test
    void create_withoutConcurrencyKeys_createsTheReader() {
        assertThat(provider.create(configuration().build())).isNotNull();
    }

    @Test
    void create_withConcurrencyAndWindow_createsTheReader() {
        assertThat(provider.create(configuration().with("concurrency", "8").with("window", "4").build())).isNotNull();
    }

    @Test
    void create_withAConcurrencyThatIsNotANumber_failsNamingTheKey() {
        PluginConfiguration invalid = configuration().with("concurrency", "many").build();

        assertThatThrownBy(() -> provider.create(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("concurrency must be a whole number of at least 1, got 'many'");
    }

    @Test
    void create_withAWindowOfZero_failsNamingTheKey() {
        PluginConfiguration invalid = configuration().with("window", "0").build();

        assertThatThrownBy(() -> provider.create(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("window must be a whole number of at least 1, got '0'");
    }
}
