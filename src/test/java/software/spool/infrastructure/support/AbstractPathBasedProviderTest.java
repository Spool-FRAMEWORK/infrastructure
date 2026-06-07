package software.spool.infrastructure.support;

import org.junit.jupiter.api.Test;
import software.spool.infrastructure.fixture.PluginConfigurationMother;
import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractPathBasedProviderTest<P extends Plugin<?>> {

    protected abstract P createProvider();
    protected abstract String expectedName();
    protected abstract int expectedPriority();
    protected abstract Class<?> expectedProductType();

    @Test
    void name_returnsExpectedValue() {
        assertThat(createProvider().name()).isEqualTo(expectedName());
    }

    @Test
    void priority_returnsExpectedValue() {
        assertThat(createProvider().priority()).isEqualTo(expectedPriority());
    }

    @Test
    void supports_configWithPath_returnsTrue() {
        PluginConfiguration config = PluginConfigurationMother.withPath("/test/path");
        assertThat(createProvider().supports(config)).isTrue();
    }

    @Test
    void supports_emptyConfig_returnsFalse() {
        assertThat(createProvider().supports(PluginConfigurationMother.empty())).isFalse();
    }

    @Test
    void create_withPathConfig_returnsExpectedType() {
        PluginConfiguration config = PluginConfigurationMother.withPath("/test/path");
        Object result = createProvider().create(config);
        assertThat(result).isNotNull().isInstanceOf(expectedProductType());
    }
}
