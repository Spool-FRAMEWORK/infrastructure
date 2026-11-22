package software.spool.infrastructure.adapter.pollsource.http;

import software.spool.core.exception.PieceAsksToWaitException;
import software.spool.core.exception.SpoolException;
import software.spool.crawler.api.port.source.PollSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

public class HTTPPollSource implements PollSource<byte[]> {
    private static final Set<Integer> STATUSES_THAT_ASK_TO_WAIT = Set.of(429, 503);

    private final HttpClient httpClient;
    private final String url;
    private final String sourceId;
    private final RetryAfter retryAfter;
    private final Clock clock;

    public HTTPPollSource(String url, String sourceId) {
        this(url, sourceId, RetryAfter.byDefault(), Clock.systemUTC());
    }

    public HTTPPollSource(String url, String sourceId, RetryAfter retryAfter, Clock clock) {
        this.url = url;
        this.sourceId = sourceId;
        this.retryAfter = retryAfter;
        this.clock = clock;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @Override
    public byte[] fetch() throws SpoolException {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            HttpResponse<byte[]> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofByteArray()
            );

            if (STATUSES_THAT_ASK_TO_WAIT.contains(response.statusCode())) {
                Optional<Instant> until = retryAfter.waitUntil(response.headers(), response.body(), clock.instant());
                if (until.isPresent()) throw new PieceAsksToWaitException(sourceId, until.get());
            }

            if (response.statusCode() != 200) {
                throw new RuntimeException(
                        sourceId + "returned HTTP " + response.statusCode()
                );
            }

            return response.body();
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error polling: " + e.getMessage(), e);
        }
    }

    @Override
    public String sourceId() {
        return sourceId;
    }
}
