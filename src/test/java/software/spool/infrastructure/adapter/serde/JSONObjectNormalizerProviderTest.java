package software.spool.infrastructure.adapter.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.spool.crawler.internal.utils.factory.Normalizer;
import software.spool.infrastructure.fixture.PluginConfigurationMother;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JSONObjectNormalizerProviderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final JSONObjectNormalizerProvider provider = new JSONObjectNormalizerProvider();

    @Mock
    private PluginConfiguration config;

    @Test
    void name_returnsJSON_OBJECT_NORMALIZER() {
        assertThat(provider.name()).isEqualTo("JSON_OBJECT_NORMALIZER");
    }

    @Test
    void priority_returns10() {
        assertThat(provider.priority()).isEqualTo(10);
    }

    @Test
    void supports_rulesAndRootPathPresent_returnsTrue() {
        when(config.has("rules")).thenReturn(true);
        when(config.has("rootPath")).thenReturn(true);
        assertThat(provider.supports(config)).isTrue();
    }

    @Test
    void supports_rulesMissing_returnsFalse() {
        when(config.has("rules")).thenReturn(false);
        assertThat(provider.supports(config)).isFalse();
    }

    @Test
    void supports_rootPathMissing_returnsFalse() {
        when(config.has("rules")).thenReturn(true);
        when(config.has("rootPath")).thenReturn(false);
        assertThat(provider.supports(config)).isFalse();
    }

    @Test
    void supports_emptyConfig_returnsFalse() {
        assertThat(provider.supports(PluginConfigurationMother.empty())).isFalse();
    }

    @Test
    void create_withValidConfig_returnsNonNullNormalizer() {
        assertThat(provider.create(PluginConfigurationMother.withRulesAndRootPath("[]", "data"))).isNotNull();
    }

    @Test
    void normalize_aSingleObject_producesOneRecordEqualToIt() {
        Normalizer<?> normalizer = provider.create(PluginConfigurationMother.withRulesAndRootPath("[]", ""));

        List<JsonNode> records = normalize(normalizer, "{\"id\":1,\"name\":\"a\"}");

        assertThat(records).containsExactly(json("{\"id\":1,\"name\":\"a\"}"));
    }

    @Test
    void normalize_withARootPath_producesTheObjectFoundThere() {
        Normalizer<?> normalizer = provider.create(PluginConfigurationMother.withRulesAndRootPath("[]", "data"));

        List<JsonNode> records = normalize(normalizer, "{\"data\":{\"id\":1},\"meta\":{\"origin\":\"x\"}}");

        assertThat(records).containsExactly(json("{\"id\":1}"));
    }

    @Test
    void normalize_withARule_addsTheFieldFoundOutsideTheRootPath() {
        String rules = "[{\"source\":\"meta.origin\",\"target\":\"origin\"}]";
        Normalizer<?> normalizer = provider.create(PluginConfigurationMother.withRulesAndRootPath(rules, "data"));

        List<JsonNode> records = normalize(normalizer, "{\"data\":{\"id\":1},\"meta\":{\"origin\":\"x\"}}");

        assertThat(records).containsExactly(json("{\"id\":1,\"origin\":\"x\"}"));
    }

    @Test
    void normalize_withAnEmptyRootPath_usesTheWholeDocument() {
        Normalizer<?> normalizer = provider.create(PluginConfigurationMother.withRulesAndRootPath("[]", ""));

        List<JsonNode> records = normalize(normalizer, "{\"data\":{\"id\":1},\"meta\":{\"origin\":\"x\"}}");

        assertThat(records).containsExactly(json("{\"data\":{\"id\":1},\"meta\":{\"origin\":\"x\"}}"));
    }

    @Test
    void normalize_theSameObjectWithTheArrayNormalizer_producesNoRecords() {
        Normalizer<?> normalizer = new JSONArrayNormalizerProvider()
                .create(PluginConfigurationMother.withRulesAndRootPath("[]", ""));

        assertThat(normalize(normalizer, "{\"id\":1,\"name\":\"a\"}")).isEmpty();
    }

    @SuppressWarnings("unchecked")
    private static List<JsonNode> normalize(Normalizer<?> normalizer, String payload) {
        return ((Normalizer<byte[]>) normalizer).normalize(payload.getBytes(StandardCharsets.UTF_8))
                .map(JSONObjectNormalizerProviderTest::parse)
                .toList();
    }

    private static JsonNode json(String text) {
        return parse(text.getBytes(StandardCharsets.UTF_8));
    }

    private static JsonNode parse(byte[] bytes) {
        try {
            return MAPPER.readTree(bytes);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}
