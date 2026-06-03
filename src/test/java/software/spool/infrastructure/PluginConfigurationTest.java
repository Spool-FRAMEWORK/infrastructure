package software.spool.infrastructure;

import org.junit.jupiter.api.Test;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PluginConfigurationTest {

    @Test
    void empty_hasNoProperties() {
        PluginConfiguration config = PluginConfiguration.empty();
        assertThat(config.has("anything")).isFalse();
    }

    @Test
    void empty_getReturnsEmptyOptional() {
        assertThat(PluginConfiguration.empty().get("key")).isEmpty();
    }

    @Test
    void builder_singleStringProperty_accessible() {
        PluginConfiguration config = PluginConfiguration.builder()
                .with("host", "localhost")
                .build();

        assertThat(config.has("host")).isTrue();
        assertThat(config.get("host")).hasValue("localhost");
    }

    @Test
    void builder_multipleProperties_allAccessible() {
        PluginConfiguration config = PluginConfiguration.builder()
                .with("k1", "v1")
                .with("k2", "v2")
                .build();

        assertThat(config.get("k1")).hasValue("v1");
        assertThat(config.get("k2")).hasValue("v2");
    }

    @Test
    void builder_contextObject_accessible() {
        Object obj = new Object();
        PluginConfiguration config = PluginConfiguration.builder()
                .with("ctx", obj)
                .build();

        assertThat(config.get("ctx", Object.class)).contains(obj);
    }

    @Test
    void builder_returnsImmutableCopy_mutatingSourceMapDoesNotAffectConfig() {
        Map<String, String> mutable = new HashMap<>();
        mutable.put("key", "original");

        PluginConfiguration config = PluginConfiguration.of(mutable);
        mutable.put("key", "mutated");

        assertThat(config.require("key")).isEqualTo("original");
    }

    @Test
    void get_presentProperty_returnsValue() {
        PluginConfiguration config = PluginConfiguration.builder().with("x", "hello").build();
        assertThat(config.get("x")).hasValue("hello");
    }

    @Test
    void get_absentProperty_returnsEmpty() {
        assertThat(PluginConfiguration.empty().get("missing")).isEmpty();
    }

    @Test
    void get_contextWithCorrectType_returnsValue() {
        Integer num = 42;
        PluginConfiguration config = PluginConfiguration.builder().with("n", num).build();
        assertThat(config.get("n", Integer.class)).contains(42);
    }

    @Test
    void get_contextAbsentKey_returnsEmpty() {
        assertThat(PluginConfiguration.empty().get("absent", Object.class)).isEmpty();
    }

    @Test
    void get_contextWithWrongType_throwsClassCastException() {
        PluginConfiguration config = PluginConfiguration.builder().with("n", 42).build();
        assertThatThrownBy(() -> config.get("n", String.class))
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    void require_presentKey_returnsValue() {
        PluginConfiguration config = PluginConfiguration.builder().with("url", "http://a").build();
        assertThat(config.require("url")).isEqualTo("http://a");
    }

    @Test
    void require_absentKey_throwsIllegalArgument() {
        assertThatThrownBy(() -> PluginConfiguration.empty().require("missing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing");
    }

    @Test
    void require_contextPresentKey_returnsTypedValue() {
        Object obj = new Object();
        PluginConfiguration config = PluginConfiguration.builder().with("obj", obj).build();
        assertThat(config.require("obj", Object.class)).isSameAs(obj);
    }

    @Test
    void require_contextAbsentKey_throwsIllegalArgument() {
        assertThatThrownBy(() -> PluginConfiguration.empty().require("missing", Object.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing");
    }

    @Test
    void has_presentKey_returnsTrue() {
        PluginConfiguration config = PluginConfiguration.builder().with("flag", "on").build();
        assertThat(config.has("flag")).isTrue();
    }

    @Test
    void has_absentKey_returnsFalse() {
        assertThat(PluginConfiguration.empty().has("nope")).isFalse();
    }

    @Test
    void of_propertiesMap_allPropertiesAccessible() {
        PluginConfiguration config = PluginConfiguration.of(Map.of("a", "1", "b", "2"));

        assertThat(config.get("a")).hasValue("1");
        assertThat(config.get("b")).hasValue("2");
    }

    @Test
    void of_propertiesAndContextMaps_bothAccessible() {
        Object ctx = new Object();
        PluginConfiguration config = PluginConfiguration.of(Map.of("k", "v"), Map.of("o", ctx));

        assertThat(config.require("k")).isEqualTo("v");
        assertThat(config.require("o", Object.class)).isSameAs(ctx);
    }

    @Test
    void of_nullMaps_treatedAsEmpty() {
        PluginConfiguration config = PluginConfiguration.of(null, null);
        assertThat(config.has("anything")).isFalse();
    }

    @Test
    void toString_containsClassNameAndProperties() {
        PluginConfiguration config = PluginConfiguration.builder().with("k", "v").build();
        assertThat(config.toString())
                .contains("PluginConfiguration")
                .contains("k");
    }
}
