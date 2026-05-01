package software.spool.infrastructure;

import software.spool.infrastructure.spi.provider.EventBusProvider;

public class Main {
    public static void main(String[] args) {
        PluginRegistry.get(EventBusProvider.class, "MEMORY")
                .create();
    }
}
