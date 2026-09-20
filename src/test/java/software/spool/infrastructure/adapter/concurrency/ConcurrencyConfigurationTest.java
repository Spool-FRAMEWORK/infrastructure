package software.spool.infrastructure.adapter.concurrency;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.infrastructure.adapter.dataLake.filesystem.FileSystemPartitionedReaderProvider;
import software.spool.infrastructure.adapter.datamart.FileSystemDataMartWriterProvider;
import software.spool.infrastructure.adapter.datamart.RawFileSystemDataMartWriterProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConcurrencyConfigurationTest {

    private static BoundedConcurrency read(String... keysAndValues) {
        PluginConfiguration.Builder builder = PluginConfiguration.builder();
        for (int i = 0; i < keysAndValues.length; i += 2) builder.with(keysAndValues[i], keysAndValues[i + 1]);
        return ConcurrencyConfiguration.from(builder.build());
    }

    @Test
    void withoutKeysThereIsOneThreadPerProcessorAndTheWindowIsAsLargeAsThePool() {
        try (BoundedConcurrency concurrency = read()) {
            assertThat(concurrency.threads()).isEqualTo(Runtime.getRuntime().availableProcessors());
            assertThat(concurrency.window()).isEqualTo(concurrency.threads());
        }
    }

    @Test
    void concurrencyIsTheNumberOfThreads() {
        try (BoundedConcurrency concurrency = read("concurrency", "16")) {
            assertThat(concurrency.threads()).isEqualTo(16);
            assertThat(concurrency.window()).isEqualTo(16);
        }
    }

    @Test
    void theWindowCanBeSmallerThanThePool() {
        try (BoundedConcurrency concurrency = read("concurrency", "16", "window", "4")) {
            assertThat(concurrency.threads()).isEqualTo(16);
            assertThat(concurrency.window()).isEqualTo(4);
        }
    }

    @Test
    void theWindowIsLoweredToThePoolWhenItIsLarger() {
        try (BoundedConcurrency concurrency = read("concurrency", "4", "window", "64")) {
            assertThat(concurrency.window()).isEqualTo(4);
        }
    }

    @Test
    void aValueThatIsNotAWholeNumberIsRejectedSayingWhichKeyAndWhatWasGiven() {
        assertThatThrownBy(() -> read("concurrency", "many"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("concurrency must be a whole number of at least 1, got 'many'");
    }

    @Test
    void zeroThreadsAreRejected() {
        assertThatThrownBy(() -> read("concurrency", "0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("concurrency must be a whole number of at least 1, got '0'");
    }

    @Test
    void aWindowThatIsNotAtLeastOneIsRejected() {
        assertThatThrownBy(() -> read("window", "-3"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("window must be a whole number of at least 1, got '-3'");
    }

    @Test
    void theProvidersOfTheFileSystemReadTheKeys(@TempDir Path tmp) {
        PluginConfiguration configuration = PluginConfiguration.builder()
                .with("path", tmp.toString()).with("concurrency", "2").build();

        assertThat(new FileSystemPartitionedReaderProvider().create(configuration)).isNotNull();
        assertThat(new FileSystemDataMartWriterProvider().create(configuration)).isNotNull();
        assertThat(new RawFileSystemDataMartWriterProvider().create(configuration)).isNotNull();
    }

    @Test
    void theProvidersOfTheFileSystemRejectAWrongValue(@TempDir Path tmp) {
        PluginConfiguration configuration = PluginConfiguration.builder()
                .with("path", tmp.toString()).with("concurrency", "many").build();

        assertThatThrownBy(() -> new FileSystemPartitionedReaderProvider().create(configuration))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FileSystemDataMartWriterProvider().create(configuration))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawFileSystemDataMartWriterProvider().create(configuration))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
