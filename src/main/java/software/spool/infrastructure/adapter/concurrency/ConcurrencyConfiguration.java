package software.spool.infrastructure.adapter.concurrency;

import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.util.OptionalInt;

/**
 * Reads how many files a provider reads or writes at a time from its configuration.
 *
 * <p>{@value #CONCURRENCY} is the number of threads, one per available processor when it is not given. {@value #WINDOW}
 * is how many of them may be in flight, as large as the pool when it is not given and never larger than it.
 * A value that is not a whole number of at least 1 is rejected when the descriptor is loaded.</p>
 */
public final class ConcurrencyConfiguration {

    public static final String CONCURRENCY = "concurrency";
    public static final String WINDOW = "window";

    private ConcurrencyConfiguration() {}

    /**
     * Creates the concurrency the configuration describes. The caller owns it.
     *
     * @param configuration the configuration of the provider
     * @return the concurrency
     * @throws IllegalArgumentException if a key holds something other than a whole number of at least 1
     */
    public static BoundedConcurrency from(PluginConfiguration configuration) {
        int threads = positive(configuration, CONCURRENCY).orElse(Runtime.getRuntime().availableProcessors());
        int window = positive(configuration, WINDOW).orElse(threads);
        return BoundedConcurrency.withThreads(threads, window);
    }

    private static OptionalInt positive(PluginConfiguration configuration, String key) {
        return configuration.get(key).map(value -> OptionalInt.of(parse(key, value))).orElse(OptionalInt.empty());
    }

    private static int parse(String key, String value) {
        try {
            int number = Integer.parseInt(value.trim());
            if (number >= 1) return number;
        } catch (NumberFormatException ignored) {
            throw invalid(key, value);
        }
        throw invalid(key, value);
    }

    private static IllegalArgumentException invalid(String key, String value) {
        return new IllegalArgumentException(key + " must be a whole number of at least 1, got '" + value + "'");
    }
}
