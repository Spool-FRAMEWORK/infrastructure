package software.spool.infrastructure.adapter.dataLake.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.health.HealthCheck;
import software.spool.core.port.health.HealthStatus;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("docker")
class PostgreSqlHealthProbeDatabaseTest {
    private static final String HOST = "localhost";
    private static final int PORT = 5432;
    private static final String TABLE = "envelope_headers_probe_test";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final PartitionKey PARTITION = PartitionKey.ofEntriesWithoutDate().with("source", "boe").build();

    private static HikariDataSource dataSource;

    @BeforeAll
    static void connect() {
        Assumptions.assumeTrue(isReachable(), "PostgreSQL not available: run docker compose -f .docker/docker-compose.yml up -d postgres");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url());
        config.setUsername("spool");
        config.setPassword("spool");
        config.setMaximumPoolSize(2);
        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void disconnect() {
        if (dataSource != null) dataSource.close();
    }

    @BeforeEach
    @AfterEach
    void dropTable() throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE);
        }
    }

    @Test
    void probe_tableOfTheStore_isHealthy() {
        new PostgreSqlEnvelopeHeaderStore(dataSource, TABLE);

        HealthCheck check = new PostgreSqlHealthProbe(dataSource, TABLE, TIMEOUT).probe();

        assertThat(check.status()).isEqualTo(HealthStatus.HEALTHY);
        assertThat(check.name()).isEqualTo("POSTGRESQL");
    }

    @Test
    void probe_tableMissing_isUnhealthyAndNamesTheTable() {
        HealthCheck check = new PostgreSqlHealthProbe(dataSource, TABLE, TIMEOUT).probe();

        assertThat(check.status()).isEqualTo(HealthStatus.UNHEALTHY);
        assertThat(check.reason()).contains(TABLE);
    }

    @Test
    void probe_tableWithHeaders_keepsThem() {
        PostgreSqlEnvelopeHeaderStore store = new PostgreSqlEnvelopeHeaderStore(dataSource, TABLE);
        EnvelopeHeader header = header("a");
        store.write(PARTITION, header);

        new PostgreSqlHealthProbe(dataSource, TABLE, TIMEOUT).probe();

        assertThat(store.findIn(PARTITION)).containsExactly(header);
    }

    @Test
    void provider_pieceWhoseStoreIsBuilt_isHealthy() {
        PluginConfiguration configuration = PluginConfiguration.of(Map.of(
                "url", url(), "user", "spool", "password", "spool", "table", TABLE, "poolSize", "2"));
        new PostgreSqlEnvelopeHeaderWriterProvider().create(configuration);

        HealthCheck check = new PostgreSqlHealthProbeProvider().create(configuration).probe();

        assertThat(check.status()).isEqualTo(HealthStatus.HEALTHY);
    }

    private static EnvelopeHeader header(String key) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        return new EnvelopeHeader(IdempotencyKey.of(key), new EventMetadata().set(EventMetadataKey.SOURCE, "boe"),
                MediaType.of("application/pdf"), EnvelopeStatus.PERSISTED, 1, now, now);
    }

    private static String url() {
        return "jdbc:postgresql://" + HOST + ":" + PORT + "/spool";
    }

    private static boolean isReachable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(HOST, PORT), 1000);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
