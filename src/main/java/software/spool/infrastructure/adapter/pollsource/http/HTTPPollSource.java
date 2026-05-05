package software.spool.infrastructure.adapter.pollsource.http;

import software.spool.core.exception.SpoolException;
import software.spool.crawler.api.port.source.PollSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class HTTPPollSource implements PollSource<String> {
    private final HttpClient httpClient;
    private final String url;
    private final String sourceId;

    public HTTPPollSource(String url, String sourceId) {
        this.url = url;
        this.sourceId = sourceId;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @Override
    public String poll() throws SpoolException {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofString()
            );

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
