package software.spool.infrastructure.fixture;

import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

public class StubPlugin<T> implements Plugin<T> {

    private final String name;
    private final int priority;
    private final boolean supports;
    private final T product;

    protected StubPlugin(String name, int priority, boolean supports, T product) {
        this.name = name;
        this.priority = priority;
        this.supports = supports;
        this.product = product;
    }

    public static <T> StubPlugin<T> of(String name, int priority, T product) {
        return new StubPlugin<>(name, priority, true, product);
    }

    public static <T> StubPlugin<T> of(String name, int priority, boolean supports, T product) {
        return new StubPlugin<>(name, priority, supports, product);
    }

    @Override public String name() { return name; }
    @Override public int priority() { return priority; }
    @Override public boolean supports(PluginConfiguration configuration) { return supports; }
    @Override public T create(PluginConfiguration configuration) { return product; }
}
