package software.spool.infrastructure.adapter.pollsource.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.http.HttpHeaders;
import java.util.Optional;

/** Where a source answers how long to wait: a header, or a field of a JSON body. */
public sealed interface RetryAfterLocation {

    Optional<String> find(HttpHeaders headers, byte[] body);

    record Header(String name) implements RetryAfterLocation {
        @Override
        public Optional<String> find(HttpHeaders headers, byte[] body) {
            return headers.firstValue(name);
        }
    }

    /** A field of the body, named by a JSON Pointer such as {@code /error/retryInSeconds}. */
    record BodyField(String pointer) implements RetryAfterLocation {
        private static final ObjectMapper JSON = new ObjectMapper();

        @Override
        public Optional<String> find(HttpHeaders headers, byte[] body) {
            try {
                JsonNode field = JSON.readTree(body).at(pointer);
                return field.isValueNode() ? Optional.of(field.asText()) : Optional.empty();
            } catch (IOException notJson) {
                return Optional.empty();
            }
        }
    }
}
