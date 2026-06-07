package software.spool.infrastructure.adapter.serde;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.spool.infrastructure.fixture.PluginConfigurationMother;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JSONNormalizerProviderTest {

    private final JSONNormalizerProvider provider = new JSONNormalizerProvider();

    @Mock
    private PluginConfiguration config;

    @Test
    void name_returnsJSON_NORMALIZER() {
        assertThat(provider.name()).isEqualTo("JSON_NORMALIZER");
    }

    @Test
    void priority_returns10() {
        assertThat(provider.priority()).isEqualTo(10);
    }

    @Test
    void supports_rulesAndRootPathPresent_returnsTrue() {
        when(config.has("rules")).thenReturn(true);
        when(config.has("rootPath")).thenReturn(true);
        assertThat(provider.supports(config)).isTrue();
    }

    @Test
    void supports_rulesMissing_returnsFalse() {
        when(config.has("rules")).thenReturn(false);
        assertThat(provider.supports(config)).isFalse();
    }

    @Test
    void supports_rootPathMissing_returnsFalse() {
        when(config.has("rules")).thenReturn(true);
        when(config.has("rootPath")).thenReturn(false);
        assertThat(provider.supports(config)).isFalse();
    }

    @Test
    void supports_bothMissing_returnsFalse() {
        when(config.has("rules")).thenReturn(false);
        assertThat(provider.supports(config)).isFalse();
    }

    @Test
    void supports_emptyConfig_returnsFalse() {
        assertThat(provider.supports(PluginConfigurationMother.empty())).isFalse();
    }

    @Test
    void create_withValidConfig_returnsNonNullNormalizer() {
        PluginConfiguration realConfig = PluginConfigurationMother.withRulesAndRootPath("[]", "$.data");
        Object result = provider.create(realConfig);
        assertThat(result).isNotNull();
    }
}
