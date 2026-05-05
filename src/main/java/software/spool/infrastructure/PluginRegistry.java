package software.spool.infrastructure;

import software.spool.infrastructure.spi.Plugin;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class PluginRegistry {
    private static final Map<Class<?>, Map<String, Object>> REGISTRY = new ConcurrentHashMap<>();

    private PluginRegistry() {}

    public static <T extends Plugin<R>, R> void register(Class<T> type, T plugin) {
        REGISTRY.computeIfAbsent(type, k -> new ConcurrentHashMap<>())
                .put(plugin.name().toUpperCase(), plugin);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Plugin<R>, R> Optional<T> find(Class<T> type, String name) {
        Map<String, Object> map = REGISTRY.get(type);
        if (map == null) return Optional.empty();
        return Optional.ofNullable((T) map.get(name.toUpperCase()));
    }

    @SuppressWarnings("unchecked")
    public static <T extends Plugin<R>, R> Map<String, T> findAll(Class<T> type) {
        Map<String, Object> map = REGISTRY.getOrDefault(type, Map.of());
        Map<String, T> result = new ConcurrentHashMap<>();
        map.forEach((k, v) -> result.put(k, (T) v));
        return result;
    }

    public static boolean hasAny(Class<?> type) {
        return REGISTRY.containsKey(type) && !REGISTRY.get(type).isEmpty();
    }
}