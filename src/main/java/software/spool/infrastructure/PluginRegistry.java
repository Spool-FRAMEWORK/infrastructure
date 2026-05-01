package software.spool.infrastructure;


import software.spool.infrastructure.spi.Plugin;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class PluginRegistry {
    private static final Map<Class<?>, Map<String, Object>> REGISTRY = new ConcurrentHashMap<>();

    private PluginRegistry() {}

    static {
        PluginBootStrap.init();
    }

    public static <T extends Plugin> void register(Class<T> type, T plugin) {
        REGISTRY
                .computeIfAbsent(type, k -> new ConcurrentHashMap<>())
                .put(plugin.name().toUpperCase(), plugin);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Plugin> T get(Class<T> type, String name) {
        Map<String, Object> map = REGISTRY.get(type);
        if (map == null)
            throw new IllegalStateException("No plugins registered for: " + type.getSimpleName());
        T plugin = (T) map.get(name.toUpperCase());
        if (plugin == null)
            throw new IllegalStateException("No plugins registered for: " + type.getSimpleName() + " | " + name);
        return plugin;
    }
}