package software.spool.infrastructure.spi.provider;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class PluginConfiguration {
    private final Map<String, String> properties;
    private final Map<String, Object> context;

    private PluginConfiguration(Map<String, String> properties, Map<String, Object> context) {
        this.properties = Objects.isNull(properties) ? Map.of() : Map.copyOf(properties);
        this.context = Objects.isNull(context) ? Map.of() : Map.copyOf(context);
    }

    public static PluginConfiguration empty() {
        return new PluginConfiguration(Map.of(), Map.of());
    }

    public static PluginConfiguration of(Map<String, String> properties, Map<String, Object> context) {
        return new PluginConfiguration(properties, context);
    }

    public static PluginConfiguration of(Map<String, String> properties) {
        return new PluginConfiguration(properties, Map.of());
    }

    public <T> Optional<T> get(String key, Class<T> type) {
        return Optional.ofNullable(type.cast(context.get(key)));
    }

    public <T> T require(String key, Class<T> type) {
        return get(key, type).orElseThrow(
                () -> new IllegalArgumentException("Missing context key: " + key));
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(properties.get(key));
    }

    public String require(String key) {
        return get(key).orElseThrow(() ->
                new IllegalArgumentException("Missing required configuration key: " + key));
    }

    public boolean has(String key) {
        return properties.containsKey(key);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final Map<String, String> map = new HashMap<>();
        private final Map<String, Object> context = new HashMap<>();

        public Builder with(String key, String value) {
            map.put(key, value);
            return this;
        }

        public Builder with(String key, Object value) {   // ← nuevo
            context.put(key, value);
            return this;
        }

        public PluginConfiguration build() {
            return new PluginConfiguration(map, context);
        }
    }

    @Override
    public String toString() {
        return "PluginConfiguration{" +
                "properties=" + properties +
                ", context=" + context +
                '}';
    }
}