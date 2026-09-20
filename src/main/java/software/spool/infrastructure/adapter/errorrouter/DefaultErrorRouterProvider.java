package software.spool.infrastructure.adapter.errorrouter;

import software.spool.core.utils.routing.ErrorRouter;
import software.spool.crawler.api.utils.CrawlerErrorRouter;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.ErrorRouterProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

/**
 * The router a crawler uses when none is chosen: a duplicate event is rejected with an info line and any other
 * error is logged as an error. Choosing {@code DEFAULT} in a descriptor is the same as leaving the key out.
 */
@SpoolPlugin(ErrorRouterProvider.class)
public class DefaultErrorRouterProvider implements ErrorRouterProvider {

    @Override
    public String name() {
        return "DEFAULT";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return true;
    }

    @Override
    public ErrorRouter create(PluginConfiguration configuration) {
        return CrawlerErrorRouter.defaults(null);
    }
}
