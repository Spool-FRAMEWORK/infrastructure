package software.spool.infrastructure.adapter.bus.memory;

import org.junit.jupiter.api.Test;
import software.spool.core.port.bus.EventBus;
import software.spool.infrastructure.fixture.PluginConfigurationMother;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryEventBusProviderTest {

    private final InMemoryEventBusProvider provider = new InMemoryEventBusProvider();

    @Test
    void name_returnsIN_MEMORY() {
        assertThat(provider.name()).isEqualTo("IN_MEMORY");
    }

    @Test
    void priority_returns100() {
        assertThat(provider.priority()).isEqualTo(100);
    }

    @Test
    void supports_emptyConfig_returnsTrue() {
        assertThat(provider.supports(PluginConfigurationMother.empty())).isTrue();
    }

    @Test
    void supports_anyConfig_alwaysReturnsTrue() {
        assertThat(provider.supports(PluginConfigurationMother.withPath("/some/path"))).isTrue();
        assertThat(provider.supports(PluginConfigurationMother.withBootstrapServers("localhost:9092"))).isTrue();
    }

    @Test
    void create_returnsNonNullEventBus() {
        EventBus result = provider.create(PluginConfigurationMother.empty());
        assertThat(result).isNotNull().isInstanceOf(EventBus.class);
    }

    @Test
    void create_returnsSingletonInstance() {
        EventBus first = provider.create(PluginConfigurationMother.empty());
        EventBus second = provider.create(PluginConfigurationMother.empty());
        assertThat(first).isSameAs(second);
    }
}
