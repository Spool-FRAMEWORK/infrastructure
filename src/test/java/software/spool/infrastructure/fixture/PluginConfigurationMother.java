package software.spool.infrastructure.fixture;

import software.spool.infrastructure.spi.provider.PluginConfiguration;

public final class PluginConfigurationMother {

    private PluginConfigurationMother() {}

    public static PluginConfiguration empty() {
        return PluginConfiguration.empty();
    }

    public static PluginConfiguration withPath(String path) {
        return PluginConfiguration.builder().with("path", path).build();
    }

    public static PluginConfiguration withBootstrapServers(String servers) {
        return PluginConfiguration.builder().with("bootstrap.servers", servers).build();
    }

    public static PluginConfiguration withUrlAndSourceId(String url, String sourceId) {
        return PluginConfiguration.builder()
                .with("url", url)
                .with("sourceId", sourceId)
                .build();
    }

    public static PluginConfiguration withEventClassName(String eventClassName, String sourceId) {
        return PluginConfiguration.builder()
                .with("eventClassName", eventClassName)
                .with("sourceId", sourceId)
                .build();
    }

    public static PluginConfiguration withRulesAndRootPath(String rules, String rootPath) {
        return PluginConfiguration.builder()
                .with("rules", rules)
                .with("rootPath", rootPath)
                .build();
    }

    public static PluginConfiguration withContextObject(String key, Object value) {
        return PluginConfiguration.builder().with(key, value).build();
    }
}
