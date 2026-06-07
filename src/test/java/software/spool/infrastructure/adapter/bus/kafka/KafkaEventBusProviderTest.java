package software.spool.infrastructure.adapter.bus.kafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.spool.core.port.bus.EventBus;
import software.spool.infrastructure.fixture.PluginConfigurationMother;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaEventBusProviderTest {
    private final KafkaEventBusProvider provider = new KafkaEventBusProvider();

    @Mock
    private PluginConfiguration config;

    @Test
    void name_returnsKAFKA() {
        assertThat(provider.name()).isEqualTo("KAFKA");
    }

    @Test
    void priority_returns10() {
        assertThat(provider.priority()).isEqualTo(10);
    }

    @Test
    void supports_configHasBootstrapServers_returnsTrue() {
        when(config.has("bootstrap.servers")).thenReturn(true);
        assertThat(provider.supports(config)).isTrue();
    }

    @Test
    void supports_configMissingBootstrapServers_returnsFalse() {
        when(config.has("bootstrap.servers")).thenReturn(false);
        assertThat(provider.supports(config)).isFalse();
    }

    @Test
    void supports_emptyConfig_returnsFalse() {
        assertThat(provider.supports(PluginConfigurationMother.empty())).isFalse();
    }

    @Test
    void priority_isLowerThanInMemory() {
        assertThat(provider.priority()).isLessThan(100);
    }

    @Test
    void create_withBootstrapServers_returnsKafkaEventBusInstance() {
        PluginConfiguration realConfig = PluginConfigurationMother.withBootstrapServers("localhost:9092");
        EventBus result = provider.create(realConfig);
        assertThat(result).isNotNull().isInstanceOf(KafkaEventBus.class);
    }
}
