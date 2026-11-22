package software.spool.infrastructure.adapter.dataLake.postgresql;

import java.util.regex.Pattern;

/**
 * The name of a PostgreSQL table that can go into a statement as it is: a plain identifier, so it never needs quoting
 * and never carries SQL of its own.
 */
record TableName(String value) {
    private static final Pattern PLAIN_IDENTIFIER = Pattern.compile("[a-z_][a-z0-9_]{0,62}");

    TableName {
        if (!PLAIN_IDENTIFIER.matcher(value).matches())
            throw new IllegalArgumentException("Table name " + value
                    + " is not valid: it must start with a lowercase letter or an underscore and have only lowercase letters, digits and underscores, up to 63 characters");
    }

    @Override
    public String toString() {
        return value;
    }
}
