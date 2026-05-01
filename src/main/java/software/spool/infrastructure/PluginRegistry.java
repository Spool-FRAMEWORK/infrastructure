package software.spool.infrastructure;

import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.provider.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class PluginRegistry {
    private static final Map<Class<?>, Map<String, Object>> REGISTRY = new ConcurrentHashMap<>();

    private PluginRegistry() {}

    static {
        ServiceLoader.load(EventBusProvider.class)
                .forEach(p -> register(EventBusProvider.class, p));
        ServiceLoader.load(InboxWriterProvider.class)
                .forEach(p -> register(InboxWriterProvider.class, p));
        ServiceLoader.load(InboxReaderProvider.class)
                .forEach(p -> register(InboxReaderProvider.class, p));
        ServiceLoader.load(InboxUpdaterProvider.class)
                .forEach(p -> register(InboxUpdaterProvider.class, p));
        ServiceLoader.load(InboxEnvelopeRemoverProvider.class)
                .forEach(p -> register(InboxEnvelopeRemoverProvider.class, p));
        ServiceLoader.load(DataLakeWriterProvider.class)
                .forEach(p -> register(DataLakeWriterProvider.class, p));
    }

    public static <T extends Plugin<R>, R> void register(Class<T> type, T plugin) {
        REGISTRY.computeIfAbsent(type, k -> new ConcurrentHashMap<>())
                .put(plugin.name().toUpperCase(), plugin);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Plugin<R>, R> T get(Class<T> type, String name) {
        Map<String, Object> map = REGISTRY.get(type);
        if (map == null)
            throw new IllegalStateException("No plugins registered for: " + type.getSimpleName());
        T plugin = (T) map.get(name.toUpperCase());
        if (plugin == null)
            throw new IllegalStateException("No plugin '" + name + "' for: " + type.getSimpleName());
        return plugin;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Plugin<R>, R> R resolve(Class<T> type, PluginConfiguration configuration) {
        Map<String, Object> map = REGISTRY.get(type);
        if (map == null)
            throw new IllegalStateException("No plugins registered for: " + type.getSimpleName());
        return map.values().stream()
                .map(p -> (T) p)
                .filter(p -> p.supports(configuration))
                .min(Comparator.comparingInt(Plugin::priority))
                .orElseThrow(() -> new IllegalStateException(
                        "No plugin supports the given configuration for: " + type.getSimpleName()
                ))
                .create(configuration);
    }
}