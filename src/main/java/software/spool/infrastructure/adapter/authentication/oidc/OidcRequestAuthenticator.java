package software.spool.infrastructure.adapter.authentication.oidc;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import software.spool.core.port.authentication.Authentication;
import software.spool.core.port.authentication.Identity;
import software.spool.core.port.authentication.RequestAuthenticator;
import software.spool.core.port.http.HttpRequest;

import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.text.ParseException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Identifies the caller from an OpenID Connect token in {@code Authorization: Bearer} (ADR-013). The token's
 * signature is checked against the issuer's public keys (JWKS), and so are its issuer, audience and expiry, all
 * with Nimbus JOSE + JWT. The roles are read from a claim path such as {@code realm_access.roles}. When no JWKS
 * address is given, it is taken from the issuer's discovery document the first time a token arrives. A rejection
 * never repeats the token.
 */
public final class OidcRequestAuthenticator implements RequestAuthenticator {
    private static final String BEARER = "Bearer ";
    private static final String DISCOVERY = "/.well-known/openid-configuration";
    private static final Set<JWSAlgorithm> ALGORITHMS = Set.of(
            JWSAlgorithm.RS256, JWSAlgorithm.RS384, JWSAlgorithm.RS512,
            JWSAlgorithm.PS256, JWSAlgorithm.ES256, JWSAlgorithm.ES384);

    private final String name;
    private final String issuer;
    private final String audience;
    private final Optional<String> jwksUri;
    private final String rolesClaim;
    private volatile ConfigurableJWTProcessor<SecurityContext> processor;

    public OidcRequestAuthenticator(String name, String issuer, String audience, String jwksUri, String rolesClaim) {
        this(name, issuer, audience, Optional.of(jwksUri), rolesClaim);
    }

    public OidcRequestAuthenticator(String name, String issuer, String audience, Optional<String> jwksUri, String rolesClaim) {
        this.name = name;
        this.issuer = issuer;
        this.audience = audience;
        this.jwksUri = jwksUri;
        this.rolesClaim = rolesClaim;
    }

    @Override
    public Authentication authenticate(HttpRequest request) {
        Optional<String> token = request.header("Authorization")
                .filter(value -> value.startsWith(BEARER))
                .map(value -> value.substring(BEARER.length()).trim());
        if (token.isEmpty()) return Authentication.rejected("the request has no bearer token");
        try {
            JWTClaimsSet claims = processor().process(token.get(), null);
            return Authentication.accepted(new Identity(claims.getSubject(), rolesOf(claims), name));
        } catch (ParseException e) {
            return Authentication.rejected("the bearer value is not a signed JWT");
        } catch (BadJOSEException e) {
            return Authentication.rejected("the token is not valid: " + e.getMessage());
        } catch (JOSEException | IllegalStateException e) {
            return Authentication.rejected("the token could not be checked: " + e.getMessage());
        }
    }

    private ConfigurableJWTProcessor<SecurityContext> processor() {
        ConfigurableJWTProcessor<SecurityContext> current = processor;
        if (current != null) return current;
        synchronized (this) {
            if (processor == null) processor = processorFor(jwksUri.orElseGet(this::discoverJwksUri));
            return processor;
        }
    }

    private ConfigurableJWTProcessor<SecurityContext> processorFor(String keysAddress) {
        try {
            JWKSource<SecurityContext> keys = JWKSourceBuilder.create(new URL(keysAddress)).retrying(true).build();
            DefaultJWTProcessor<SecurityContext> built = new DefaultJWTProcessor<>();
            built.setJWSKeySelector(new JWSVerificationKeySelector<>(ALGORITHMS, keys));
            built.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<>(audience,
                    new JWTClaimsSet.Builder().issuer(issuer).build(), Set.of("sub", "exp")));
            return built;
        } catch (java.net.MalformedURLException e) {
            throw new IllegalStateException("the JWKS address '" + keysAddress + "' is not a URL");
        }
    }

    private String discoverJwksUri() {
        String address = issuer.replaceAll("/+$", "") + DISCOVERY;
        try {
            HttpResponse<String> response = HttpClient.newHttpClient().send(
                    java.net.http.HttpRequest.newBuilder(URI.create(address)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("the issuer's discovery document at " + address + " answered " + response.statusCode());
            }
            String keys = JSONObjectUtils.getString(JSONObjectUtils.parse(response.body()), "jwks_uri");
            if (keys == null) throw new IllegalStateException("the issuer's discovery document at " + address + " has no jwks_uri");
            return keys;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("reading the issuer's discovery document at " + address + " was interrupted");
        } catch (java.io.IOException | ParseException e) {
            throw new IllegalStateException("the issuer's discovery document at " + address + " could not be read: " + e.getMessage());
        }
    }

    private Set<String> rolesOf(JWTClaimsSet claims) {
        Object value = claims.getClaims();
        for (String segment : rolesClaim.split("\\.")) {
            if (!(value instanceof Map<?, ?> map)) return Set.of();
            value = map.get(segment);
        }
        if (value instanceof Collection<?> roles) {
            Set<String> names = new LinkedHashSet<>();
            roles.forEach(role -> names.add(String.valueOf(role)));
            return names;
        }
        if (value instanceof String role) return Set.of(role.split(" "));
        return Set.of();
    }
}
