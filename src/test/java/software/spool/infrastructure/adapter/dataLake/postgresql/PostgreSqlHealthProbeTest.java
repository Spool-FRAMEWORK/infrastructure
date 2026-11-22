package software.spool.infrastructure.adapter.dataLake.postgresql;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import software.spool.core.port.health.HealthCheck;
import software.spool.core.port.health.HealthStatus;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgreSqlHealthProbeTest {
    private static final int PORT_WHERE_NOTHING_LISTENS = 1;

    @Test
    void probe_databaseThatDoesNotAnswer_isUnhealthyAndNamesTheTable() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setServerNames(new String[]{"localhost"});
        dataSource.setPortNumbers(new int[]{PORT_WHERE_NOTHING_LISTENS});
        dataSource.setDatabaseName("spool");
        dataSource.setConnectTimeout(1);

        HealthCheck check = new PostgreSqlHealthProbe(dataSource, "envelope_headers", Duration.ofSeconds(1)).probe();

        assertThat(check.status()).isEqualTo(HealthStatus.UNHEALTHY);
        assertThat(check.name()).isEqualTo("POSTGRESQL");
        assertThat(check.reason()).contains("envelope_headers");
    }

    @Test
    void constructor_tableNameThatIsNotAPlainIdentifier_isRejected() {
        assertThatThrownBy(() -> new PostgreSqlHealthProbe(new PGSimpleDataSource(), "headers; drop table x", Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("headers; drop table x");
    }

    @Test
    void provider_supportsAPieceWithUrl() {
        PostgreSqlHealthProbeProvider provider = new PostgreSqlHealthProbeProvider();

        assertThat(provider.name()).isEqualTo("POSTGRESQL");
        assertThat(provider.supports(PluginConfiguration.of(Map.of("url", "jdbc:postgresql://localhost:5432/spool")))).isTrue();
        assertThat(provider.supports(PluginConfiguration.of(Map.of("user", "spool")))).isFalse();
    }
}
