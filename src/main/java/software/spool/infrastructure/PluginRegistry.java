package software.spool.infrastructure;

import software.spool.infrastructure.spi.Plugin;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class PluginRegistry {
    private static final Map<Class<?>, Map<String, Object>> REGISTRY = new ConcurrentHashMap<>();

    private PluginRegistry() {}

    /**
     * Registers a plugin under its name, ignoring case.
     *
     * <p>When another plugin already has that name, the one with the lower priority number is kept, so a provider of
     * your own can replace one of the framework by declaring a lower number. Registering the same class again changes
     * nothing.</p>
     *
     * @param type   the port the plugin belongs to
     * @param plugin the plugin to register
     * @throws PluginConflictException if two different plugins have the same name and the same priority
     */
    public static <T extends Plugin<R>, R> void register(Class<T> type, T plugin) {
        REGISTRY.computeIfAbsent(type, k -> new ConcurrentHashMap<>())
                .merge(plugin.name().toUpperCase(), plugin,
                        (registered, candidate) -> preferred(type, (Plugin<?>) registered, (Plugin<?>) candidate));
    }

    private static Plugin<?> preferred(Class<?> type, Plugin<?> registered, Plugin<?> candidate) {
        if (registered.getClass().getName().equals(candidate.getClass().getName())) return registered;
        if (candidate.priority() < registered.priority()) return candidate;
        if (candidate.priority() > registered.priority()) return registered;
        throw new PluginConflictException(type, registered, candidate);
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