package software.spool.infrastructure.scan;

import software.spool.infrastructure.PluginRegistry;
import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.SpoolPlugin;

final class PluginCandidateLoader {

    private PluginCandidateLoader() {}

    @SuppressWarnings("unchecked")
    static <T extends Plugin<R>, R> void tryRegister(String className, Class<T> type, ClassLoader cl) {
        try {
            Class<?> candidate = Class.forName(className, false, cl);
            SpoolPlugin annotation = candidate.getAnnotation(SpoolPlugin.class);
            if (annotation == null) return;
            if (!annotation.value().isAssignableFrom(candidate)) return;
            if (!type.isAssignableFrom(candidate)) return;
            T instance = (T) candidate.getDeclaredConstructor().newInstance();
            PluginRegistry.register(type, instance);
        } catch (NoClassDefFoundError | Exception ignored) {}
    }
}