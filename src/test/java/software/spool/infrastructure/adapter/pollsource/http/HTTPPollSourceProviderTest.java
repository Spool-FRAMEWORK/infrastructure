package software.spool.infrastructure.adapter.pollsource.http;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.spool.crawler.api.port.source.PollSource;
import software.spool.infrastructure.fixture.PluginConfigurationMother;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
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
}
