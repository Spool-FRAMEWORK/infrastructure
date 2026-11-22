package software.spool.infrastructure.adapter.pollsource.http;

import software.spool.core.utils.time.ReadableDuration;
import software.spool.crawler.api.port.source.PollSource;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.PollSourceProvider;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

@SpoolPlugin(PollSourceProvider.class)
public class HTTPPollSourceProvider implements PollSourceProvider {
    static final String RETRY_AFTER_HEADER = "retryAfterHeader";
    static final String RETRY_AFTER_FIELD = "retryAfterField";
    static final String RETRY_AFTER_FORMAT = "retryAfterFormat";
    static final String MAX_RETRY_AFTER = "maxRetryAfter";

    @Override
    public String name() {
        return "HTTP";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("url") && configuration.has("sourceId");
    }

    @Override
    public PollSource<?> create(PluginConfiguration configuration) {
        return new HTTPPollSource(configuration.require("url"), configuration.require("sourceId"),
                retryAfterOf(configuration), Clock.systemUTC());
    }

    private static RetryAfter retryAfterOf(PluginConfiguration configuration) {
        return new RetryAfter(locationOf(configuration), formatOf(configuration.get(RETRY_AFTER_FORMAT)),
                maximumOf(configuration.get(MAX_RETRY_AFTER)));
    }

    private static RetryAfterLocation locationOf(PluginConfiguration configuration) {
        Optional<String> header = configuration.get(RETRY_AFTER_HEADER);
        Optional<String> field = configuration.get(RETRY_AFTER_FIELD);
        if (header.isPresent() && field.isPresent()) {
            throw new IllegalArgumentException("A source declares either " + RETRY_AFTER_HEADER + " or "
                    + RETRY_AFTER_FIELD + " as where its wait comes from, not both.");
        }
        return field.<RetryAfterLocation>map(RetryAfterLocation.BodyField::new)
                .orElseGet(() -> new RetryAfterLocation.Header(header.orElse(RetryAfter.DEFAULT_HEADER)));
    }

    private static RetryAfterFormat formatOf(Optional<String> format) {
        return format.map(name -> Arrays.stream(RetryAfterFormat.values())
                        .filter(known -> known.name().equals(name.strip().toUpperCase(Locale.ROOT)))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException(RETRY_AFTER_FORMAT + " " + name
                                + " is not a known format. Known formats: " + Arrays.toString(RetryAfterFormat.values()) + ".")))
                .orElse(RetryAfterFormat.SECONDS);
    }

    private static Duration maximumOf(Optional<String> maximum) {
        return maximum.map(value -> ReadableDuration.parse(value).orElseThrow(() -> new IllegalArgumentException(
                        MAX_RETRY_AFTER + " is not a duration such as 30s or 1h: " + value)))
                .orElse(RetryAfter.DEFAULT_MAXIMUM);
    }
}
