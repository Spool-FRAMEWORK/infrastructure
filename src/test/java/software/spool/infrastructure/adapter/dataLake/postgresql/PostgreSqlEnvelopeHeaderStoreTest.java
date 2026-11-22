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
import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.EnvelopeHeaderReader;
import software.spool.core.port.datalake.EnvelopeHeaderRemover;
import software.spool.core.port.datalake.EnvelopeHeaderWriter;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("docker")
class PostgreSqlEnvelopeHeaderStoreTest {
    private static final String HOST = "localhost";
    private static final int PORT = 5432;
    private static final String TABLE = "envelope_headers_test";
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    private static final PartitionKey DAY_1 = PartitionKey.ofEntriesWithoutDate()
            .with("source", "boe").with("year", 2026).with("month", 11).with("day", 1).build();
    private static final PartitionKey DAY_10 = PartitionKey.ofEntriesWithoutDate()
            .with("source", "boe").with("year", 2026).with("month", 11).with("day", 10).build();
    private static final PartitionKey DAY_1_HOUR_9 = PartitionKey.ofEntriesWithoutDate()
            .with("source", "boe").with("year", 2026).with("month", 11).with("day", 1).with("hour", 9).build();

    private static HikariDataSource dataSource;
    private PostgreSqlEnvelopeHeaderStore store;

    @BeforeAll
    static void connect() {
        Assumptions.assumeTrue(isReachable(), "PostgreSQL not available: run docker compose -f .docker/docker-compose.yml up -d postgres");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + HOST + ":" + PORT + "/spool");
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
    void createStore() throws SQLException {
        dropTable();
        store = new PostgreSqlEnvelopeHeaderStore(dataSource, TABLE);
    }

    @AfterEach
    void cleanUp() throws SQLException {
        dropTable();
    }

    @Test
    void constructor_tableMissing_createsIt() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             ResultSet tables = connection.getMetaData().getTables(null, null, TABLE, null)) {
            assertThat(tables.next()).isTrue();
        }
    }

    @Test
    void constructor_tableAlreadyThere_keepsItsHeaders() {
        store.write(DAY_1, header("a", EnvelopeStatus.PERSISTED));

        PostgreSqlEnvelopeHeaderStore again = new PostgreSqlEnvelopeHeaderStore(dataSource, TABLE);

        assertThat(again.find(IdempotencyKey.of("a"))).isPresent();
    }

    @Test
    void constructor_tableNameThatIsNotAPlainIdentifier_isRejected() {
        assertThatThrownBy(() -> new PostgreSqlEnvelopeHeaderStore(dataSource, "headers; drop table x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("headers; drop table x");
    }

    @Test
    void find_writtenHeader_returnsItWhole() {
        EnvelopeHeader header = header("a", EnvelopeStatus.PERSISTED);

        store.write(DAY_1, header);

        assertThat(store.find(IdempotencyKey.of("a"))).contains(header);
    }

    @Test
    void find_unknownKey_returnsEmpty() {
        assertThat(store.find(IdempotencyKey.of("missing"))).isEmpty();
    }

    @Test
    void write_sameKeyAgain_replacesTheHeader() {
        store.write(DAY_1, header("a", EnvelopeStatus.PERSISTED));
        EnvelopeHeader quarantined = header("a", EnvelopeStatus.QUARANTINED);

        store.write(DAY_1, quarantined);

        assertThat(store.find(IdempotencyKey.of("a"))).contains(quarantined);
        assertThat(store.findIn(DAY_1)).containsExactly(quarantined);
    }

    @Test
    void findIn_partition_returnsItsHeadersAndThoseBelowIt() {
        EnvelopeHeader inDay = header("a", EnvelopeStatus.PERSISTED);
        EnvelopeHeader inHour = header("b", EnvelopeStatus.PERSISTED);
        store.write(DAY_1, inDay);
        store.write(DAY_1_HOUR_9, inHour);

        assertThat(store.findIn(DAY_1)).containsExactlyInAnyOrder(inDay, inHour);
    }

    @Test
    void findIn_partition_leavesOutAPartitionWhoseValueOnlyStartsTheSame() {
        store.write(DAY_1, header("a", EnvelopeStatus.PERSISTED));
        store.write(DAY_10, header("b", EnvelopeStatus.PERSISTED));

        assertThat(store.findIn(DAY_1)).extracting(EnvelopeHeader::idempotencyKey)
                .containsExactly(IdempotencyKey.of("a"));
    }

    @Test
    void findIn_partitionWithCharactersThatAreWildcardsInSql_matchesThemLiterally() {
        PartitionKey withWildcards = PartitionKey.ofEntriesWithoutDate().with("source", "b_e%").build();
        PartitionKey lookingAlike = PartitionKey.ofEntriesWithoutDate().with("source", "bxe%x").build();
        store.write(withWildcards, header("a", EnvelopeStatus.PERSISTED));
        store.write(lookingAlike, header("b", EnvelopeStatus.PERSISTED));

        assertThat(store.findIn(withWildcards)).extracting(EnvelopeHeader::idempotencyKey)
                .containsExactly(IdempotencyKey.of("a"));
    }

    @Test
    void remove_headerInItsPartition_removesIt() {
        store.write(DAY_1, header("a", EnvelopeStatus.PERSISTED));

        store.remove(DAY_1, IdempotencyKey.of("a"));

        assertThat(store.find(IdempotencyKey.of("a"))).isEmpty();
    }

    @Test
    void remove_headerOfAnotherPartition_keepsIt() {
        store.write(DAY_1, header("a", EnvelopeStatus.PERSISTED));

        store.remove(DAY_10, IdempotencyKey.of("a"));

        assertThat(store.find(IdempotencyKey.of("a"))).isPresent();
    }

    @Test
    void remove_unknownKey_doesNothing() {
        store.remove(DAY_1, IdempotencyKey.of("missing"));

        assertThat(store.findIn(DAY_1)).isEmpty();
    }

    @Test
    void providers_sameConfiguration_shareOneStore() {
        PluginConfiguration configuration = PluginConfiguration.of(Map.of(
                "url", "jdbc:postgresql://" + HOST + ":" + PORT + "/spool", "user", "spool", "password", "spool",
                "table", TABLE, "poolSize", "2"));

        EnvelopeHeaderWriter writer = new PostgreSqlEnvelopeHeaderWriterProvider().create(configuration);
        EnvelopeHeaderReader reader = new PostgreSqlEnvelopeHeaderReaderProvider().create(configuration);
        EnvelopeHeaderRemover remover = new PostgreSqlEnvelopeHeaderRemoverProvider().create(configuration);

        assertThat(reader).isSameAs(writer).isSameAs(remover);
        writer.write(DAY_1, header("a", EnvelopeStatus.PERSISTED));
        assertThat(reader.find(IdempotencyKey.of("a"))).isPresent();
    }

    private static EnvelopeHeader header(String key, EnvelopeStatus status) {
        EventMetadata metadata = new EventMetadata().set(EventMetadataKey.SOURCE, "boe");
        return new EnvelopeHeader(IdempotencyKey.of(key), metadata, MediaType.of("application/pdf"), status, 1,
                NOW, NOW.plusSeconds(5));
    }

    private static void dropTable() throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE);
        }
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
