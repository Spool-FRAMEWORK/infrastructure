package software.spool.infrastructure.adapter.quarantine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.spool.infrastructure.fixture.PluginConfigurationMother;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FileSystemQuarantineStoreProviderTest {

    private final FileSystemQuarantineStoreProvider provider = new FileSystemQuarantineStoreProvider();

    @Mock
    private PluginConfiguration config;

    @Test
    void name_returnsFILE_SYSTEM() {
        assertThat(provider.name()).isEqualTo("FILE_SYSTEM");
    }

    @Test
    void priority_returns10() {
        assertThat(provider.priority()).isEqualTo(10);
    }

    @Test
    void supports_pathPresent_returnsTrue() {
        when(config.has("path")).thenReturn(true);
        assertThat(provider.supports(config)).isTrue();
    }

    @Test
    void supports_pathMissing_returnsFalse() {
        when(config.has("path")).thenReturn(false);
        assertThat(provider.supports(config)).isFalse();
    }

    @Test
    void supports_emptyConfig_returnsFalse() {
        assertThat(provider.supports(PluginConfigurationMother.empty())).isFalse();
    }

    @Test
    void create_withValidConfig_returnsNonNullStore() {
        assertThat(provider.create(PluginConfigurationMother.withPath("target/spool-test/quarantine"))).isNotNull();
    }
}
