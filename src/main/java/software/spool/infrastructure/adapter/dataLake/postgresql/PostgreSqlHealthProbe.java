package software.spool.infrastructure.adapter.dataLake.postgresql;

import software.spool.core.port.health.HealthCheck;
import software.spool.core.port.health.HealthProbe;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

/**
 * Checks that the table of a piece answers and can be read, asking for a single row of it. It writes nothing, so what
 * the table keeps does not change.
 */
public final class PostgreSqlHealthProbe implements HealthProbe {
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);
    private static final String NAME = "POSTGRESQL";

    private final DataSource dataSource;
    private final TableName table;
    private final Duration timeout;

    public PostgreSqlHealthProbe(DataSource dataSource, String table, Duration timeout) {
        this.dataSource = dataSource;
        this.table = new TableName(table);
        this.timeout = timeout;
    }

    @Override
    public HealthCheck probe() {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(Math.toIntExact(Math.max(1, timeout.toSeconds())));
            statement.executeQuery("SELECT 1 FROM " + table + " LIMIT 1").close();
            return HealthCheck.healthy(NAME);
        } catch (SQLException e) {
            return HealthCheck.unhealthy(NAME, "The table " + table + " cannot be read: " + e.getMessage());
        }
    }
}
