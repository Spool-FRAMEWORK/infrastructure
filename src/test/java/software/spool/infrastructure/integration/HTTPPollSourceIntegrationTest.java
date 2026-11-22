package software.spool.infrastructure.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.spool.core.exception.PieceAsksToWaitException;
import software.spool.infrastructure.adapter.pollsource.http.HTTPPollSource;
import software.spool.infrastructure.adapter.pollsource.http.RetryAfter;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.*;

class HTTPPollSourceIntegrationTest {

    static WireMockServer server;

    @BeforeAll
    static void startServer() {
        server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
    }

    @AfterAll
    static void stopServer() {
        server.stop();
    }

    @AfterEach
    void resetStubs() {
        server.resetAll();
    }

    @Test
    void fetch_200_returnsResponseBody() throws Exception {
        server.stubFor(get(urlEqualTo("/data"))
                .willReturn(aResponse().withStatus(200).withBody("response-bytes")));

        HTTPPollSource source = new HTTPPollSource(serverUrl("/data"), "src");

        byte[] result = source.fetch();

        assertThat(result).isEqualTo("response-bytes".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void fetch_404_throwsRuntimeException() {
        server.stubFor(get(urlEqualTo("/data"))
                .willReturn(aResponse().withStatus(404)));

        HTTPPollSource source = new HTTPPollSource(serverUrl("/data"), "src");

        assertThatThrownBy(source::fetch)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("404");
    }

    @Test
    void fetch_connectionRefused_throwsRuntimeException() {
        HTTPPollSource source = new HTTPPollSource("http://localhost:19999/no-server", "src");

        assertThatThrownBy(source::fetch).isInstanceOf(RuntimeException.class);
    }

    @Test
    void fetch_sendsAcceptJsonHeader() throws Exception {
        server.stubFor(get(urlEqualTo("/data"))
                .willReturn(aResponse().withStatus(200).withBody("ok")));

        HTTPPollSource source = new HTTPPollSource(serverUrl("/data"), "src");
        source.fetch();

        server.verify(getRequestedFor(urlEqualTo("/data"))
                .withHeader("Accept", equalTo("application/json")));
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 503})
    void fetch_whenTheSourceAsksToWait_saysUntilWhen(int status) {
        server.stubFor(get(urlEqualTo("/data"))
                .willReturn(aResponse().withStatus(status).withHeader("Retry-After", "3")));
        Instant now = Instant.parse("2026-11-22T10:00:00Z");

        HTTPPollSource source = new HTTPPollSource(serverUrl("/data"), "src", RetryAfter.byDefault(),
                Clock.fixed(now, ZoneOffset.UTC));

        assertThatThrownBy(source::fetch)
                .isInstanceOfSatisfying(PieceAsksToWaitException.class,
                        wait -> assertThat(wait.until()).isEqualTo(now.plusSeconds(3)));
    }

    @Test
    void fetch_500WithRetryAfter_isAFailureAndNotAWait() {
        server.stubFor(get(urlEqualTo("/data"))
                .willReturn(aResponse().withStatus(500).withHeader("Retry-After", "3")));

        HTTPPollSource source = new HTTPPollSource(serverUrl("/data"), "src");

        assertThatThrownBy(source::fetch).isNotInstanceOf(PieceAsksToWaitException.class).hasMessageContaining("500");
    }

    @Test
    void fetch_429WithoutSayingHowLong_isAFailure() {
        server.stubFor(get(urlEqualTo("/data")).willReturn(aResponse().withStatus(429)));

        HTTPPollSource source = new HTTPPollSource(serverUrl("/data"), "src");

        assertThatThrownBy(source::fetch).isNotInstanceOf(PieceAsksToWaitException.class).hasMessageContaining("429");
    }

    private String serverUrl(String path) {
        return "http://localhost:" + server.port() + path;
    }
}
