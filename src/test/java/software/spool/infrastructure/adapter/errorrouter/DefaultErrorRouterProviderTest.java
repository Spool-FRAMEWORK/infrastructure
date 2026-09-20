package software.spool.infrastructure.adapter.errorrouter;

import org.junit.jupiter.api.Test;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.infrastructure.PluginRegistry;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.provider.ErrorRouterProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultErrorRouterProviderTest {

    @Test
    void itIsRegisteredUnderTheNameDefault() {
        ErrorRouterProvider provider = PluginResolver.get(ErrorRouterProvider.class, "DEFAULT");

        assertThat(provider).isInstanceOf(DefaultErrorRouterProvider.class);
        assertThat(PluginRegistry.findAll(ErrorRouterProvider.class)).containsKey("DEFAULT");
    }

    @Test
    void itSupportsAnyConfiguration() {
        assertThat(new DefaultErrorRouterProvider().supports(PluginConfiguration.empty())).isTrue();
    }

    @Test
    void itCreatesTheRouterTheCrawlerUsesByDefault() {
        ErrorRouter router = new DefaultErrorRouterProvider().create(PluginConfiguration.empty());

        assertThat(router).isNotNull();
    }
}
