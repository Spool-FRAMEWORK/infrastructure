package software.spool.infrastructure.adapter.dataLake.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import software.spool.core.port.health.HealthProbe;
import software.spool.infrastructure.spi.provider.InvalidPluginConfigurationException;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Builds the PostgreSQL store of envelope headers that a configuration describes, once per configuration, so the
 * writer, the reader, the remover and the health check of the same piece share one store and one pool of
 * connections.
 *
 * <p>{@value #URL} is the JDBC URL of the database and is required. {@value #USER} and {@value #PASSWORD} are the
 * credentials, {@value #TABLE} the table, {@value #DEFAULT_TABLE} when it is not given, and {@value #POOL_SIZE} how
 * many connections the pool keeps at most, {@value #DEFAULT_POOL_SIZE} when it is not given.</p>
 */
final class PostgreSqlEnvelopeHeaderStores {
    static final String TYPE = "POSTGRESQL";
    static final String URL = "url";
    static final String USER = "user";
    static final String PASSWORD = "password";
    static final String TABLE = "table";
    static final String POOL_SIZE = "poolSize";
    static final String DEFAULT_TABLE = "envelope_headers";
    static final int DEFAULT_POOL_SIZE = 4;
    private static final String POSTGRESQL_URL_PREFIX = "jdbc:postgresql:";

    private static final Map<Settings, Piece> PIECES = new ConcurrentHashMap<>();

    private PostgreSqlEnvelopeHeaderStores() {
    }

    static PostgreSqlEnvelopeHeaderStore storeOf(PluginConfiguration configuration) {
        return pieceOf(configuration).store();
    }

    static HealthProbe healthProbeOf(PluginConfiguration configuration) {
        Piece piece = pieceOf(configuration);
        return new PostgreSqlHealthProbe(piece.dataSource(), piece.table(), PostgreSqlHealthProbe.DEFAULT_TIMEOUT);
    }

    private static Piece pieceOf(PluginConfiguration configuration) {
        return PIECES.computeIfAbsent(Settings.of(configuration), PostgreSqlEnvelopeHeaderStores::create);
    }

    private static Piece create(Settings settings) {
        HikariConfig pool = new HikariConfig();
        pool.setJdbcUrl(settings.url());
        settings.user().ifPresent(pool::setUsername);
        settings.password().ifPresent(pool::setPassword);
        pool.setMaximumPoolSize(settings.poolSize());
        pool.setPoolName("spool-envelope-headers-" + settings.table());
        HikariDataSource dataSource = new HikariDataSource(pool);
        return new Piece(dataSource, settings.table(), new PostgreSqlEnvelopeHeaderStore(dataSource, settings.table()));
    }

    private record Piece(HikariDataSource dataSource, String table, PostgreSqlEnvelopeHeaderStore store) {
    }

    private record Settings(String url, Optional<String> user, Optional<String> password,
                            String table, int poolSize) {
        static Settings of(PluginConfiguration configuration) {
            String url = configuration.require(URL);
            if (!url.startsWith(POSTGRESQL_URL_PREFIX))
                throw new InvalidPluginConfigurationException(URL, "must be a JDBC URL of PostgreSQL, starting with "
                        + POSTGRESQL_URL_PREFIX);
            return new Settings(url, configuration.get(USER), configuration.get(PASSWORD),
                    configuration.get(TABLE).orElse(DEFAULT_TABLE), poolSizeOf(configuration));
        }

        private static int poolSizeOf(PluginConfiguration configuration) {
            return configuration.get(POOL_SIZE).map(Settings::positive).orElse(DEFAULT_POOL_SIZE);
        }

        @Override
        public String toString() {
            return "Settings[table=" + table + ", poolSize=" + poolSize + "]";
        }

        private static int positive(String value) {
            try {
                int number = Integer.parseInt(value.trim());
                if (number >= 1) return number;
            } catch (NumberFormatException ignored) {
            }
            throw new InvalidPluginConfigurationException(POOL_SIZE, "must be a whole number of at least 1, got '" + value + "'");
        }
    }
}
