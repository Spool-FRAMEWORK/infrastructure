package software.spool.infrastructure.adapter.dataLake.postgresql;

import java.net.URI;
import java.util.Optional;

/**
 * The port and the database a JDBC URL of PostgreSQL points at, with PostgreSQL's own when the URL does not say them
 * or cannot be read, as when it is a Terraform variable.
 */
record PostgreSqlAddress(int port, String database) {
    static final int DEFAULT_PORT = 5432;
    static final String DEFAULT_DATABASE = "postgres";
    private static final String JDBC_PREFIX = "jdbc:";

    static PostgreSqlAddress of(String url) {
        return uriOf(url)
                .map(uri -> new PostgreSqlAddress(uri.getPort() > 0 ? uri.getPort() : DEFAULT_PORT, databaseOf(uri)))
                .orElse(new PostgreSqlAddress(DEFAULT_PORT, DEFAULT_DATABASE));
    }

    private static Optional<URI> uriOf(String url) {
        if (!url.startsWith(JDBC_PREFIX)) return Optional.empty();
        try {
            URI uri = URI.create(url.substring(JDBC_PREFIX.length()));
            return uri.isOpaque() ? Optional.empty() : Optional.of(uri);
        } catch (IllegalArgumentException notAnAddress) {
            return Optional.empty();
        }
    }

    private static String databaseOf(URI uri) {
        return Optional.ofNullable(uri.getPath())
                .map(path -> path.replaceFirst("^/", ""))
                .filter(name -> !name.isEmpty())
                .orElse(DEFAULT_DATABASE);
    }
}
