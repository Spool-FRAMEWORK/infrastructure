package software.spool.infrastructure.adapter.authentication.oidc;

import software.spool.core.port.authentication.RequestAuthenticator;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.authentication.RequestAuthenticatorProvider;

/**
 * {@code type: OIDC}. Requires {@code issuer} and {@code audience}; {@code rolesClaim} defaults to {@code roles}, and
 * without {@code jwksUri} the keys are found through the issuer's discovery document.
 */
@SpoolPlugin(RequestAuthenticatorProvider.class)
public class OidcRequestAuthenticatorProvider implements RequestAuthenticatorProvider {
    private static final String DEFAULT_ROLES_CLAIM = "roles";

    @Override
    public String name() {
        return "OIDC";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("issuer");
    }

    @Override
    public RequestAuthenticator create(PluginConfiguration configuration) {
        return new OidcRequestAuthenticator(
                configuration.get(NAME, String.class).orElse(name()),
                configuration.require("issuer"),
                configuration.require("audience"),
                configuration.get("jwksUri"),
                configuration.get("rolesClaim").orElse(DEFAULT_ROLES_CLAIM));
    }
}
