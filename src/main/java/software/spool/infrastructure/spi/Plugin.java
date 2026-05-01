package software.spool.infrastructure.spi;

import software.spool.infrastructure.spi.provider.PluginConfiguration;

public interface Plugin<T> {
    String name();
    int priority();
    boolean supports(PluginConfiguration configuration);
    T create(PluginConfiguration configuration);
}