package software.spool.infrastructure.adapter.authentication.hmac;

import software.spool.core.port.authentication.RequestAuthenticator;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.authentication.RequestAuthenticatorProvider;

import java.time.Clock;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@code type: HMAC_SIGNATURE}. Requires {@code header} and {@code secret}, which comes from the environment
 * (ADR-011). {@code timestampHeader} is optional, with a {@code tolerance} in ISO 8601 that defaults to
 * {@code PT5M}. {@code roles} lists, separated by commas, the roles the sender holds.
 */
@SpoolPlugin(RequestAuthenticatorProvider.class)
public class HmacSignatureRequestAuthenticatorProvider implements RequestAuthenticatorProvider {
    private static final Duration DEFAULT_TOLERANCE = Duration.ofMinutes(5);

    @Override
    public String name() {
        return "HMAC_SIGNATURE";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("header") && configuration.has("secret");
    }

    @Override
    public RequestAuthenticator create(PluginConfiguration configuration) {
        return new HmacSignatureRequestAuthenticator(
                configuration.get(NAME, String.class).orElse(name()),
                configuration.require("header"),
                configuration.require("secret"),
                configuration.get("timestampHeader"),
                toleranceOf(configuration.get("tolerance")),
                rolesOf(configuration.get("roles")),
                Clock.systemUTC());
    }

    private static Duration toleranceOf(Optional<String> tolerance) {
        try {
            return tolerance.map(Duration::parse).orElse(DEFAULT_TOLERANCE);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("tolerance must be an ISO 8601 duration such as PT5M, got '" + tolerance.orElseThrow() + "'");
        }
    }

    private static Set<String> rolesOf(Optional<String> roles) {
        return roles.map(list -> Arrays.stream(list.split(",")).map(String::trim).filter(role -> !role.isEmpty())
                .collect(Collectors.toUnmodifiableSet())).orElse(Set.of());
    }
}
