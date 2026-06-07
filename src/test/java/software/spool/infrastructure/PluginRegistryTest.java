package software.spool.infrastructure;

import org.junit.jupiter.api.Test;
import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class PluginRegistryTest {

    @Test
    void register_thenFindByExactName_returnsPlugin() {
        Plugin<?> p = plugin("ALPHA");
        register(p);

        assertThat(find(p, "ALPHA")).isPresent().contains(p);
    }

    @Test
    void find_nameIsNormalisedToUpperCase_locatesPlugin() {
        Plugin<?> p = plugin("GAMMA");
        register(p);

        assertThat(find(p, "gamma")).isPresent();
        assertThat(find(p, "Gamma")).isPresent();
        assertThat(find(p, "GAMMA")).isPresent();
    }

    @Test
    void find_typeNeverRegistered_returnsEmpty() {
        assertThat(PluginRegistry.find(NeverRegisteredPlugin.class, "DELTA")).isEmpty();
    }

    @Test
    void find_registeredTypeButWrongName_returnsEmpty() {
        Plugin<?> p = plugin("EPSILON");
        register(p);

        assertThat(find(p, "ZETA")).isEmpty();
    }

    @Test
    void findAll_multiplePluginsOfSameType_returnsAll() {
        Plugin<?> one = plugin("ONE");
        Plugin<?> two = plugin("TWO");
        Plugin<?> three = plugin("THREE");

        register(one);
        register(two);
        register(three);

        assertThat(findAll(one)).containsKeys("ONE", "TWO", "THREE");
    }

    @Test
    void findAll_nothingRegisteredForType_returnsEmptyMap() {
        assertThat(PluginRegistry.findAll(NeverRegisteredPlugin.class)).isEmpty();
    }

    @Test
    void hasAny_afterRegistration_returnsTrue() {
        Plugin<?> p = plugin("IOTA");
        register(p);

        assertThat(hasAny(p)).isTrue();
    }

    @Test
    void hasAny_nothingRegistered_returnsFalse() {
        assertThat(PluginRegistry.hasAny(NeverRegistered.class)).isFalse();
    }

    @Test
    void register_sameNameTwice_secondRegistrationOverwrites() {
        Plugin<?> first = plugin("KAPPA");
        Plugin<?> second = plugin("KAPPA");

        register(first);
        register(second);

        assertThat(find(first, "KAPPA")).contains(second);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void register(Plugin<?> p) {
        PluginRegistry.register((Class) p.getClass(), p);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Optional<Plugin<?>> find(Plugin<?> p, String name) {
        return (Optional) PluginRegistry.find((Class) p.getClass(), name);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Map<String, Plugin<?>> findAll(Plugin<?> p) {
        return (Map) PluginRegistry.findAll((Class) p.getClass());
    }

    @SuppressWarnings({"rawtypes"})
    private static boolean hasAny(Plugin<?> p) {
        return PluginRegistry.hasAny((Class) p.getClass());
    }

    private static Plugin<?> plugin(String name) {
        return new Plugin<>() {
            @Override public String name() { return name; }
            @Override public int priority() { return 0; }
            @Override public boolean supports(PluginConfiguration c) { return true; }
            @Override public Object create(PluginConfiguration c) { return name; }
        };
    }

    private interface NeverRegistered {}

    @SuppressWarnings("unused")
    private interface NeverRegisteredPlugin extends Plugin<String> {}
}
