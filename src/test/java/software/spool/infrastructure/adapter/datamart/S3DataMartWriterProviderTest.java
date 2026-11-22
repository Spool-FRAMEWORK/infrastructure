package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.Test;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.mounter.api.port.DataMartWriter;

import static org.assertj.core.api.Assertions.assertThat;

class S3DataMartWriterProviderTest {

    private final S3DataMartWriterProvider provider = new S3DataMartWriterProvider();

    private static PluginConfiguration.Builder configuration() {
        return PluginConfiguration.builder()
                .with("region", "auto")
                .with("bucket", "spool")
                .with("endpoint", "http://localhost:9000");
    }

    @Test
    void name_isS3() {
        assertThat(provider.name()).isEqualTo("S3");
    }

    @Test
    void supports_configurationWithRegionBucketAndEndpoint() {
        assertThat(provider.supports(configuration().build())).isTrue();
    }

    @Test
    void supports_configurationWithAPathOnly_isFalse() {
        assertThat(provider.supports(PluginConfiguration.builder().with("path", "/tmp/spool").build())).isFalse();
    }

    @Test
    void create_withConcurrencyAndWindow_createsADataMartWriter() {
        assertThat(provider.create(configuration().with("concurrency", "8").with("window", "4").build()))
                .isInstanceOf(DataMartWriter.class);
    }
}
