package software.spool.infrastructure.spi.provider.authentication;

import software.spool.core.port.authentication.RequestAuthenticator;
import software.spool.infrastructure.spi.Plugin;

/**
 * Builds a {@link RequestAuthenticator} declared in the {@code authentication} block of a descriptor (ADR-013).
 * The descriptor names the provider, and that name reaches the plugin as the context key {@value #NAME}, so the
 * identities it gives say which provider they come from.
 */
public interface RequestAuthenticatorProvider extends Plugin<RequestAuthenticator> {
    String NAME = "name";
}
