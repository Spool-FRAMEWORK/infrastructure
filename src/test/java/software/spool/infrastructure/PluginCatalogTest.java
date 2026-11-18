package software.spool.infrastructure;

import org.junit.jupiter.api.Test;
import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxWriterProvider;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class PluginCatalogTest {

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
