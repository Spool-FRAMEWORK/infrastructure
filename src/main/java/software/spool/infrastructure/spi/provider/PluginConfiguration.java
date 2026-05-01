package software.spool.infrastructure.spi.provider;

import java.util.Map;
import java.util.Optional;

public final class PluginConfiguration {
    private final Map<String, String> properties;

    private PluginConfiguration(Map<String, String> properties) {
        this.properties = Map.copyOf(properties);
    }

    public static PluginConfiguration empty() {
        return new PluginConfiguration(Map.of());
    }

    public static PluginConfiguration of(Map<String, String> properties) {
        return new PluginConfiguration(properties);
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
        private final java.util.HashMap<String, String> map = new java.util.HashMap<>();

        public Builder with(String key, String value) {
            map.put(key, value);
            return this;
        }

        public PluginConfiguration build() {
            return new PluginConfiguration(map);
        }
    }
}