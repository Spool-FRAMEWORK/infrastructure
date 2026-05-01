package software.spool.infrastructure;

import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.provider.EventBusProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class PluginRegistry {
    private static final Map<Class<?>, Map<String, Object>> REGISTRY = new ConcurrentHashMap<>();

    private PluginRegistry() {}

    static {
        ServiceLoader.load(EventBusProvider.class)
                .forEach(p -> register(EventBusProvider.class, p));
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