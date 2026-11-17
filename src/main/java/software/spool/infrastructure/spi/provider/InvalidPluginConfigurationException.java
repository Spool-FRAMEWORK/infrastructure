package software.spool.infrastructure.spi.provider;

/**
 * A plugin was given a configuration it cannot use: a required key is missing or a value has the wrong form. It
 * names the key, so whoever built the configuration can point at it, as the descriptor does with its path.
 */
public class InvalidPluginConfigurationException extends IllegalArgumentException {
    private final String key;

    public InvalidPluginConfigurationException(String key, String problem) {
        super(key + " " + problem);
        this.key = key;
    }

    /** The configuration key with the problem. */
    public String key() {
        return key;
    }
}
