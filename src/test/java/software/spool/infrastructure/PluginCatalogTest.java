package software.spool.infrastructure;

import org.junit.jupiter.api.Test;
import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.provider.ErrorRouterProvider;
import software.spool.infrastructure.spi.provider.PollSourceProvider;
import software.spool.infrastructure.spi.provider.StreamSourceProvider;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.dataLake.DataLakeWriterProvider;
import software.spool.infrastructure.spi.provider.dataLake.PartitionedReaderProvider;
import software.spool.infrastructure.spi.provider.datamart.DataMartWriterProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxEnvelopeRemoverProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxReaderProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxUpdaterProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxWriterProvider;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class PluginCatalogTest {

    @Test
    void eventBus_hasInMemoryAndKafka() {
        assertThat(names(EventBusProvider.class)).containsExactlyInAnyOrder("IN_MEMORY", "KAFKA");
    }

    @Test
    void inbox_everyPortHasFileSystemAndS3() {
        assertThat(names(InboxWriterProvider.class)).containsExactlyInAnyOrder("FILE_SYSTEM", "S3");
        assertThat(names(InboxReaderProvider.class)).containsExactlyInAnyOrder("FILE_SYSTEM", "S3");
        assertThat(names(InboxUpdaterProvider.class)).containsExactlyInAnyOrder("FILE_SYSTEM", "S3");
        assertThat(names(InboxEnvelopeRemoverProvider.class)).containsExactlyInAnyOrder("FILE_SYSTEM", "S3");
    }

    @Test
    void dataLakeWriter_hasFileSystemAndS3() {
        assertThat(names(DataLakeWriterProvider.class)).containsExactlyInAnyOrder("FILE_SYSTEM", "S3");
    }

    @Test
    void dataMartWriter_hasFileSystemAndRawFileSystem() {
        assertThat(names(DataMartWriterProvider.class)).containsExactlyInAnyOrder("FILE_SYSTEM", "RAW_FILE_SYSTEM");
    }

    @Test
    void errorRouter_hasTheDefault() {
        assertThat(names(ErrorRouterProvider.class)).containsExactly("DEFAULT");
    }

    @Test
    void normalizer_hasTheRegisteredFormats() {
        assertThat(names(NormalizerProvider.class)).containsExactlyInAnyOrder(
                "EVENT_CLASS_ARRAY_NORMALIZER",
                "EVENT_CLASS_NORMALIZER",
                "JSON_ARRAY_NORMALIZER",
                "JSON_NORMALIZER",
                "JSON_OBJECT_NORMALIZER",
                "PDF_NORMALIZER");
    }

    @Test
    void partitionedReader_hasFileSystem() {
        assertThat(names(PartitionedReaderProvider.class)).containsExactly("FILE_SYSTEM");
    }

    @Test
    void pollSource_hasHttp() {
        assertThat(names(PollSourceProvider.class)).containsExactly("HTTP");
    }

    @Test
    void streamSource_hasInMemory() {
        assertThat(names(StreamSourceProvider.class)).containsExactly("IN_MEMORY");
    }

    @Test
    void available_listsThePluginsSortedByName() {
        List<String> names = new ArrayList<>(names(NormalizerProvider.class));

        assertThat(names).isSorted();
    }

    @Test
    void available_exposesThePriorityOfEachPlugin() {
        assertThat(PluginResolver.available(EventBusProvider.class).get("KAFKA").priority()).isEqualTo(10);
        assertThat(PluginResolver.available(EventBusProvider.class).get("IN_MEMORY").priority()).isEqualTo(100);
        assertThat(PluginResolver.available(InboxWriterProvider.class).get("FILE_SYSTEM").priority()).isEqualTo(0);
        assertThat(PluginResolver.available(InboxWriterProvider.class).get("S3").priority()).isEqualTo(10);
    }

    private static <T extends Plugin<R>, R> Set<String> names(Class<T> port) {
        return PluginResolver.available(port).keySet();
    }
}
