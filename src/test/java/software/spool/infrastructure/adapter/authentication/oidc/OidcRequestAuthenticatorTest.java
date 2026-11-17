package software.spool.infrastructure.adapter.authentication.oidc;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.spool.core.port.authentication.Authentication;
import software.spool.core.port.http.HttpMethod;
import software.spool.core.port.http.HttpRequest;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OidcRequestAuthenticatorTest {
    private static final String ISSUER = "https://identity.example/realms/spool";
    private static final String AUDIENCE = "spool-janitor";

    private RSAKey issuerKey;
    private HttpServer jwks;
    private OidcRequestAuthenticator authenticator;

    @BeforeEach
    void startTheIssuer() throws Exception {
        issuerKey = new RSAKeyGenerator(2048).keyID("issuer-key").generate();
        byte[] keys = new JWKSet(issuerKey.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
        jwks = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        jwks.createContext("/certs", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, keys.length);
            try (OutputStream body = exchange.getResponseBody()) {
                body.write(keys);
            }
        });
        jwks.start();
        String jwksUri = "http://localhost:" + jwks.getAddress().getPort() + "/certs";
        authenticator = new OidcRequestAuthenticator("people", ISSUER, AUDIENCE, jwksUri, "realm_access.roles");
    }

    @AfterEach
    void stopTheIssuer() {
        jwks.stop(0);
    }

    @Test
    void validToken_isAcceptedWithItsSubjectAndTheRolesAtTheConfiguredPath() throws Exception {
        Authentication result = authenticator.authenticate(withBearer(token(issuerKey, claims().build())));

        assertThat(result).isInstanceOfSatisfying(Authentication.Accepted.class, accepted -> {
            assertThat(accepted.identity().subject()).isEqualTo("ana");
            assertThat(accepted.identity().roles()).containsExactlyInAnyOrder("quarantine-reader", "quarantine-resolver");
            assertThat(accepted.identity().provider()).isEqualTo("people");
        });
    }

    @Test
    void expiredToken_isRejected() throws Exception {
        String token = token(issuerKey, claims().expirationTime(Date.from(Instant.now().minusSeconds(600))).build());

        assertRejectedWithoutShowing(token);
    }

    @Test
    void tokenSignedWithAnotherKey_isRejected() throws Exception {
        RSAKey otherKey = new RSAKeyGenerator(2048).keyID("issuer-key").generate();

        assertRejectedWithoutShowing(token(otherKey, claims().build()));
    }

    @Test
    void tokenFromAnotherIssuer_isRejected() throws Exception {
        assertRejectedWithoutShowing(token(issuerKey, claims().issuer("https://elsewhere.example").build()));
    }

    @Test
    void tokenForAnotherAudience_isRejected() throws Exception {
        assertRejectedWithoutShowing(token(issuerKey, claims().audience("another-service").build()));
    }

    @Test
    void requestWithoutABearerToken_isRejected() {
        Authentication result = authenticator.authenticate(new HttpRequest(HttpMethod.GET, "/spool/quarantine", Map.of(), new byte[0]));

        assertThat(result).isInstanceOf(Authentication.Rejected.class);
    }

    @Test
    void tokenThatIsNotAJwt_isRejectedWithoutShowingIt() {
        assertRejectedWithoutShowing("not-a-jwt-but-a-secret");
    }

    private void assertRejectedWithoutShowing(String token) {
        Authentication result = authenticator.authenticate(withBearer(token));

        assertThat(result).isInstanceOfSatisfying(Authentication.Rejected.class,
                rejected -> assertThat(rejected.reason()).doesNotContain(token));
    }

    private static JWTClaimsSet.Builder claims() {
        return new JWTClaimsSet.Builder()
                .subject("ana")
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .issueTime(new Date())
                .expirationTime(Date.from(Instant.now().plusSeconds(300)))
                .claim("realm_access", Map.of("roles", List.of("quarantine-reader", "quarantine-resolver")));
    }

    private static String token(RSAKey key, JWTClaimsSet claims) throws JOSEException {
        SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(), claims);
        jwt.sign(new RSASSASigner(key));
        return jwt.serialize();
    }

    private static HttpRequest withBearer(String token) {
        return new HttpRequest(HttpMethod.GET, "/spool/quarantine", Map.of("Authorization", List.of("Bearer " + token)), new byte[0]);
    }
}
