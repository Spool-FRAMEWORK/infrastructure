package software.spool.infrastructure.adapter.dataLake.postgresql;

import org.junit.jupiter.api.Test;
import software.spool.infrastructure.spi.provider.InvalidPluginConfigurationException;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgreSqlEnvelopeHeaderProvidersTest {

    @Test
    void name_ofTheThreeProviders_isPostgreSql() {
        assertThat(new PostgreSqlEnvelopeHeaderWriterProvider().name()).isEqualTo("POSTGRESQL");
        assertThat(new PostgreSqlEnvelopeHeaderReaderProvider().name()).isEqualTo("POSTGRESQL");
        assertThat(new PostgreSqlEnvelopeHeaderRemoverProvider().name()).isEqualTo("POSTGRESQL");
    }

    @Test
    void create_withoutUrl_isRejectedNamingTheKey() {
        PluginConfiguration configuration = PluginConfiguration.of(Map.of("user", "spool"));

        assertThatThrownBy(() -> new PostgreSqlEnvelopeHeaderWriterProvider().create(configuration))
                .isInstanceOf(InvalidPluginConfigurationException.class)
                .hasMessageContaining("url");
    }

    @Test
    void create_withPoolSizeThatIsNotAPositiveWholeNumber_isRejectedNamingTheKey() {
        PluginConfiguration configuration = PluginConfiguration.of(Map.of(
                "url", "jdbc:postgresql://localhost:5432/spool", "poolSize", "0"));

        assertThatThrownBy(() -> new PostgreSqlEnvelopeHeaderWriterProvider().create(configuration))
                .isInstanceOf(InvalidPluginConfigurationException.class)
                .hasMessageContaining("poolSize")
                .hasMessageContaining("0");
    }

    @Test
    void create_withUrlThatIsNotForPostgreSql_isRejectedNamingTheKey() {
        PluginConfiguration configuration = PluginConfiguration.of(Map.of("url", "jdbc:mysql://localhost:3306/spool"));

        assertThatThrownBy(() -> new PostgreSqlEnvelopeHeaderWriterProvider().create(configuration))
                .isInstanceOf(InvalidPluginConfigurationException.class)
                .hasMessageContaining("url");
    }
}
