package software.spool.infrastructure.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.*;
import software.spool.infrastructure.adapter.pollsource.http.HTTPPollSource;

import java.nio.charset.StandardCharsets;

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

    private String serverUrl(String path) {
        return "http://localhost:" + server.port() + path;
    }
}
