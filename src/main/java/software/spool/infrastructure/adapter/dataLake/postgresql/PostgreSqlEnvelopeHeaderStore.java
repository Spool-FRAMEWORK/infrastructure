package software.spool.infrastructure.adapter.dataLake.postgresql;

import software.spool.core.exception.EnvelopeHeaderStoreException;
import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.EnvelopeHeaderReader;
import software.spool.core.port.datalake.EnvelopeHeaderRemover;
import software.spool.core.port.datalake.EnvelopeHeaderWriter;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Store of envelope headers in a PostgreSQL table, one row per envelope. The table is created with its index when it
 * is not there. Besides the whole header as JSON, the row keeps its partition, status and dates in columns of their
 * own, to query them with SQL.
 */
public class PostgreSqlEnvelopeHeaderStore implements EnvelopeHeaderWriter, EnvelopeHeaderReader, EnvelopeHeaderRemover {
    private static final String ENTRY_SEPARATOR = "::";
    private static final char LIKE_ESCAPE = '\\';

    private final DataSource dataSource;
    private final TableName table;
    private final RecordSerializer<EnvelopeHeader> serializer = RecordSerializerFactory.record();
    private final PayloadDeserializer<EnvelopeHeader> deserializer = PayloadDeserializerFactory.json().as(EnvelopeHeader.class);

    public PostgreSqlEnvelopeHeaderStore(DataSource dataSource, String table) {
        this.table = new TableName(table);
        this.dataSource = dataSource;
        createTableIfMissing();
    }

    @Override
    public void write(PartitionKey partition, EnvelopeHeader header) {
        String sql = "INSERT INTO " + table + " (idempotency_key, partition_key, status, captured_at, updated_at, header)"
                + " VALUES (?, ?, ?, ?, ?, ?::jsonb)"
                + " ON CONFLICT (idempotency_key) DO UPDATE SET partition_key = EXCLUDED.partition_key,"
                + " status = EXCLUDED.status, captured_at = EXCLUDED.captured_at, updated_at = EXCLUDED.updated_at,"
                + " header = EXCLUDED.header";
        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, header.idempotencyKey().value());
            statement.setString(2, partition.value());
            statement.setString(3, header.status().name());
            statement.setObject(4, utc(header.capturedAt()));
            statement.setObject(5, utc(header.updatedAt()));
            statement.setString(6, new String(serializer.serialize(header), StandardCharsets.UTF_8));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new EnvelopeHeaderStoreException("Header of envelope " + header.idempotencyKey().value()
                    + " could not be written to table " + table, e);
        }
    }

    @Override
    public Optional<EnvelopeHeader> find(IdempotencyKey idempotencyKey) {
        String sql = "SELECT header FROM " + table + " WHERE idempotency_key = ?";
        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, idempotencyKey.value());
            return headersOf(statement).stream().findFirst();
        } catch (SQLException e) {
            throw new EnvelopeHeaderStoreException("Header of envelope " + idempotencyKey.value()
                    + " could not be read from table " + table, e);
        }
    }

    @Override
    public Stream<EnvelopeHeader> findIn(PartitionKey partition) {
        String sql = "SELECT header FROM " + table + " WHERE partition_key = ? OR partition_key LIKE ? ESCAPE '"
                + LIKE_ESCAPE + "'";
        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, partition.value());
            statement.setString(2, literalForLike(partition.value() + ENTRY_SEPARATOR) + "%");
            return headersOf(statement).stream();
        } catch (SQLException e) {
            throw new EnvelopeHeaderStoreException("Headers of partition " + partition.value()
                    + " could not be read from table " + table, e);
        }
    }

    @Override
    public void remove(PartitionKey partition, IdempotencyKey idempotencyKey) {
        String sql = "DELETE FROM " + table + " WHERE idempotency_key = ? AND partition_key = ?";
        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, idempotencyKey.value());
            statement.setString(2, partition.value());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new EnvelopeHeaderStoreException("Header of envelope " + idempotencyKey.value()
                    + " could not be removed from table " + table, e);
        }
    }

    private void createTableIfMissing() {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + table + " ("
                    + "idempotency_key TEXT PRIMARY KEY,"
                    + " partition_key TEXT NOT NULL,"
                    + " status TEXT NOT NULL,"
                    + " captured_at TIMESTAMPTZ NOT NULL,"
                    + " updated_at TIMESTAMPTZ NOT NULL,"
                    + " header JSONB NOT NULL)");
            statement.execute("CREATE INDEX IF NOT EXISTS " + table + "_partition_key ON " + table
                    + " (partition_key text_pattern_ops)");
        } catch (SQLException e) {
            throw new EnvelopeHeaderStoreException("Table " + table + " of envelope headers could not be created", e);
        }
    }

    private List<EnvelopeHeader> headersOf(PreparedStatement statement) throws SQLException {
        List<EnvelopeHeader> headers = new ArrayList<>();
        try (ResultSet rows = statement.executeQuery()) {
            while (rows.next())
                headers.add(deserializer.deserialize(rows.getString("header").getBytes(StandardCharsets.UTF_8)));
        }
        return headers;
    }

    private static OffsetDateTime utc(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    private static String literalForLike(String value) {
        StringBuilder literal = new StringBuilder();
        for (char character : value.toCharArray()) {
            if (character == LIKE_ESCAPE || character == '%' || character == '_') literal.append(LIKE_ESCAPE);
            literal.append(character);
        }
        return literal.toString();
    }
}
