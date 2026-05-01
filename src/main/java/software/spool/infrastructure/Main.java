package software.spool.infrastructure;

import software.spool.infrastructure.spi.provider.EventBusProvider;
import software.spool.infrastructure.spi.provider.InboxWriterProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

public class Main {
    public static void main(String[] args) {
        PluginConfiguration config = PluginConfiguration.builder()
                .with("bootstrap.servers", "localhost:9092")
                .build();
        PluginRegistry.resolve(InboxWriterProvider.class, PluginConfiguration.empty());
    }
}