package software.spool.infrastructure.adapter.dataLake.s3;

import org.junit.jupiter.api.Test;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.mounter.api.port.PartitionedReader;

import static org.assertj.core.api.Assertions.assertThat;

class S3PartitionedReaderProviderTest {

    private final S3PartitionedReaderProvider provider = new S3PartitionedReaderProvider();

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
    void supports_configurationWithoutBucket_isFalse() {
        assertThat(provider.supports(PluginConfiguration.builder().with("region", "auto")
                .with("endpoint", "http://localhost:9000").build())).isFalse();
    }

    @Test
    void create_withConcurrencyAndWindow_createsAPartitionedReader() {
        assertThat(provider.create(configuration().with("concurrency", "8").with("window", "4").build()))
                .isInstanceOf(PartitionedReader.class);
    }
}
