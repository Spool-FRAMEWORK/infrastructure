package software.spool.infrastructure.spi.provider;

import software.spool.core.utils.routing.ErrorRouter;
import software.spool.infrastructure.spi.Plugin;

/**
 * Provides the {@link ErrorRouter} a module sends its errors to.
 *
 * <p>A router is written in code, for instance to send an alert somewhere instead of only writing a log line,
 * and registered with {@code @SpoolPlugin(ErrorRouterProvider.class)}. A descriptor then picks it by name with
 * {@code errorRouter.type}. The configuration holds the keys the descriptor gives under
 * {@code errorRouter.configuration}, and the context holds {@code moduleId}, the id of the module the router is for.</p>
 */
public interface ErrorRouterProvider extends Plugin<ErrorRouter> {
}
