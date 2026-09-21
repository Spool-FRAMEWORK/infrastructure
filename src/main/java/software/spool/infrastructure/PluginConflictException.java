package software.spool.infrastructure;

import software.spool.infrastructure.spi.Plugin;

public final class PluginConflictException extends RuntimeException {
    public PluginConflictException(Class<?> type, Plugin<?> registered, Plugin<?> candidate) {
        super("Two plugins named '" + registered.name() + "' are registered for " + type.getSimpleName()
                + " with the same priority " + registered.priority() + ": " + registered.getClass().getName()
                + " and " + candidate.getClass().getName()
                + ". Give one of them a lower priority number to make it win, or rename it.");
    }
}
