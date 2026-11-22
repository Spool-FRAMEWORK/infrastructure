package software.spool.infrastructure.adapter.pollsource.http;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.spool.crawler.api.port.source.PollSource;
import software.spool.infrastructure.fixture.PluginConfigurationMother;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HTTPPollSourceProviderTest {

    private final HTTPPollSourceProvider provider = new HTTPPollSourceProvider();

    @Mock
    private PluginConfiguration config;

    @Test
    void name_returnsHTTP() {
        assertThat(provider.name()).isEqualTo("HTTP");
    }

    @Test
    void priority_returns10() {
        assertThat(provider.priority()).isEqualTo(10);
    }

    @Test
    void supports_urlAndSourceIdPresent_returnsTrue() {
        when(config.has("url")).thenReturn(true);
        when(config.has("sourceId")).thenReturn(true);
        assertThat(provider.supports(config)).isTrue();
    }

    @Test
    void supports_urlMissing_returnsFalse() {
        when(config.has("url")).thenReturn(false);
        assertThat(provider.supports(config)).isFalse();
    }

    @Test
    void supports_sourceIdMissing_returnsFalse() {
        when(config.has("url")).thenReturn(true);
        when(config.has("sourceId")).thenReturn(false);
        assertThat(provider.supports(config)).isFalse();
    }

    @Test
    void supports_bothMissing_returnsFalse() {
        when(config.has("url")).thenReturn(false);
        assertThat(provider.supports(config)).isFalse();
    }

    @Test
    void supports_emptyConfig_returnsFalse() {
        assertThat(provider.supports(PluginConfigurationMother.empty())).isFalse();
    }

    @Test
    void create_withUrlAndSourceId_returnsHTTPPollSource() {
        PluginConfiguration realConfig = PluginConfigurationMother
                .withUrlAndSourceId("http://api.example.com/data", "test-source");

        PollSource<?> result = provider.create(realConfig);

        assertThat(result).isNotNull().isInstanceOf(HTTPPollSource.class);
        assertThat(result.sourceId()).isEqualTo("test-source");
    }

    @Test
    void create_withAWaitFromAHeaderAndAField_isRejected() {
        PluginConfiguration both = source(Map.of("retryAfterHeader", "X-RateLimit-Reset", "retryAfterField", "/retry"));

        assertThatThrownBy(() -> provider.create(both))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryAfterHeader").hasMessageContaining("retryAfterField");
    }

    @Test
    void create_withAnUnknownFormatOfTheWait_isRejectedNamingTheFormats() {
        PluginConfiguration unknown = source(Map.of("retryAfterFormat", "MINUTES"));

        assertThatThrownBy(() -> provider.create(unknown))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("MINUTES").hasMessageContaining("EPOCH_SECONDS");
    }

    @Test
    void create_withAMaximumWaitThatIsNotADuration_isRejected() {
        PluginConfiguration notADuration = source(Map.of("maxRetryAfter", "an hour"));

        assertThatThrownBy(() -> provider.create(notADuration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetryAfter").hasMessageContaining("an hour");
    }

    @Test
    void create_withTheWaitDeclared_givesASource() {
        PluginConfiguration declared = source(Map.of("retryAfterHeader", "X-RateLimit-Reset",
                "retryAfterFormat", "epoch_seconds", "maxRetryAfter", "2s"));

        assertThat(provider.create(declared)).isInstanceOf(HTTPPollSource.class);
    }

    private static PluginConfiguration source(Map<String, String> wait) {
        Map<String, String> properties = new HashMap<>(wait);
        properties.put("url", "http://localhost/orders");
        properties.put("sourceId", "orders-api");
        return PluginConfiguration.of(properties);
    }
}
