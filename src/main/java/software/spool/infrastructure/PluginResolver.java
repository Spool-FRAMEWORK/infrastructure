package software.spool.infrastructure;

import software.spool.infrastructure.scan.ClasspathPluginScanner;
import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class PluginResolver {
    private static final Set<Class<?>> LOADED = ConcurrentHashMap.newKeySet();
    private static final ClasspathPluginScanner SCANNER = new ClasspathPluginScanner();

    private PluginResolver() {}

    public static <T extends Plugin<R>, R> T get(Class<T> type, String name) {
        ensureLoaded(type);
        return PluginRegistry.find(type, name)
                .orElseThrow(() -> new IllegalStateException(
                        "No plugin '" + name + "' registered for: " + type.getSimpleName()));
    }

    public static <T extends Plugin<R>, R> R resolve(Class<T> type, PluginConfiguration configuration) {
        ensureLoaded(type);
        return PluginRegistry.findAll(type).values().stream()
                .filter(p -> p.supports(configuration))
                .min(Comparator.comparingInt(Plugin::priority))
                .map(p -> p.create(configuration))
                .orElseThrow(() -> new IllegalStateException(
                        "No plugin supports the given configuration for: " + type.getSimpleName()));
    }

    private static <T extends Plugin<R>, R> void ensureLoaded(Class<T> type) {
        if (LOADED.add(type)) {
            SCANNER.scan(type);
        }
    }
}