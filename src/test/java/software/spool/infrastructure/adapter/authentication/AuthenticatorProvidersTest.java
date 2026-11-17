package software.spool.infrastructure.adapter.authentication;

import org.junit.jupiter.api.Test;
import software.spool.infrastructure.adapter.authentication.hmac.HmacSignatureRequestAuthenticatorProvider;
import software.spool.infrastructure.adapter.authentication.oidc.OidcRequestAuthenticatorProvider;
import software.spool.infrastructure.spi.provider.InvalidPluginConfigurationException;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthenticatorProvidersTest {

    @Test
    void oidcWithoutAnIssuer_failsNamingTheKey() {
        assertThatThrownBy(() -> new OidcRequestAuthenticatorProvider().create(PluginConfiguration.of(Map.of("audience", "spool"))))
                .isInstanceOfSatisfying(InvalidPluginConfigurationException.class,
                        e -> assertThat(e.key()).isEqualTo("issuer"));
    }

    @Test
    void hmacWithoutASecret_failsNamingTheKey() {
        assertThatThrownBy(() -> new HmacSignatureRequestAuthenticatorProvider().create(PluginConfiguration.of(Map.of("header", "X-Signature-256"))))
                .isInstanceOfSatisfying(InvalidPluginConfigurationException.class,
                        e -> assertThat(e.key()).isEqualTo("secret"));
    }

    @Test
    void hmacWithAToleranceThatIsNotADuration_failsNamingTheKey() {
        assertThatThrownBy(() -> new HmacSignatureRequestAuthenticatorProvider().create(PluginConfiguration.of(Map.of(
                "header", "X-Signature-256", "secret", "s", "timestampHeader", "X-Timestamp", "tolerance", "soon"))))
                .isInstanceOfSatisfying(InvalidPluginConfigurationException.class,
                        e -> assertThat(e.key()).isEqualTo("tolerance"));
    }

    @Test
    void providers_areRegisteredWithTheTypesOfTheDescriptor() {
        assertThat(new OidcRequestAuthenticatorProvider().name()).isEqualTo("OIDC");
        assertThat(new HmacSignatureRequestAuthenticatorProvider().name()).isEqualTo("HMAC_SIGNATURE");
    }
}
