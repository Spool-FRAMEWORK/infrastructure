package software.spool.infrastructure.integration;

import org.junit.jupiter.api.Test;
import software.spool.infrastructure.PluginRegistry;
import software.spool.infrastructure.fixture.PluginConfigurationMother;
import software.spool.infrastructure.fixture.StubPlugin;
import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PluginRegistryIntegrationTest {

    private static final class RegisterFindType extends StubPlugin<String> {
        RegisterFindType(String name, int p) { super(name, p, true, name + "-result"); }
    }

    private static final class CrossTypeA extends StubPlugin<String> {
        CrossTypeA(String name) { super(name, 1, true, name); }
    }

    private static final class CrossTypeB extends StubPlugin<Integer> {
        CrossTypeB(String name) { super(name, 1, true, 0); }
    }

    private static final class FindAllType extends StubPlugin<Integer> {
        FindAllType(String name, int p) { super(name, p, true, p); }
    }

    private static final class PriorityType extends StubPlugin<String> {
        PriorityType(String name, int p, boolean s) { super(name, p, s, name); }
    }

    private static final class SupportFilterType extends StubPlugin<String> {
        SupportFilterType(String name, int p, boolean s) { super(name, p, s, name); }
    }

    private static final class AllUnsupportedType extends StubPlugin<String> {
        AllUnsupportedType(String name, int p) { super(name, p, false, name); }
    }

    private static final class HasAnyType extends StubPlugin<String> {
        HasAnyType(String name) { super(name, 0, true, name); }
    }

    @Test
    void registeredPlugin_foundByName() {
        RegisterFindType plugin = new RegisterFindType("IMPL_ONE", 5);
        PluginRegistry.register(RegisterFindType.class, plugin);

        Optional<RegisterFindType> found = PluginRegistry.find(RegisterFindType.class, "IMPL_ONE");
        assertThat(found).isPresent().contains(plugin);
    }

    @Test
    void twoPluginsOfDifferentTypes_registeredAndFoundIndependently() {
        CrossTypeA pluginA = new CrossTypeA("CROSS_A");
        CrossTypeB pluginB = new CrossTypeB("CROSS_B");

        PluginRegistry.register(CrossTypeA.class, pluginA);
        PluginRegistry.register(CrossTypeB.class, pluginB);

        assertThat(PluginRegistry.find(CrossTypeA.class, "CROSS_A")).contains(pluginA);
        assertThat(PluginRegistry.find(CrossTypeB.class, "CROSS_B")).contains(pluginB);
        assertThat(PluginRegistry.find(CrossTypeA.class, "CROSS_B")).isEmpty();
        assertThat(PluginRegistry.find(CrossTypeB.class, "CROSS_A")).isEmpty();
    }

    @Test
    void findAll_afterRegisteringMultiple_returnsAllPlugins() {
        PluginRegistry.register(FindAllType.class, new FindAllType("LOW", 100));
        PluginRegistry.register(FindAllType.class, new FindAllType("MEDIUM", 50));
        PluginRegistry.register(FindAllType.class, new FindAllType("HIGH", 10));

        Map<String, FindAllType> all = PluginRegistry.findAll(FindAllType.class);
        assertThat(all).containsKeys("LOW", "MEDIUM", "HIGH");
    }

    @Test
    void prioritySelection_lowestPriorityNumberWins() {
        PluginRegistry.register(PriorityType.class, new PriorityType("PREF", 1, true));
        PluginRegistry.register(PriorityType.class, new PriorityType("FALL", 100, true));

        PluginConfiguration config = PluginConfigurationMother.empty();

        String winner = PluginRegistry.findAll(PriorityType.class).values().stream()
                .filter(p -> p.supports(config))
                .min(Comparator.comparingInt(Plugin::priority))
                .map(p -> p.create(config))
                .orElseThrow();

        assertThat(winner).isEqualTo("PREF");
    }

    @Test
    void prioritySelection_unsupportedPluginsExcluded() {
        PluginRegistry.register(SupportFilterType.class, new SupportFilterType("SUPPORTED", 50, true));
        PluginRegistry.register(SupportFilterType.class, new SupportFilterType("UNSUPPORTED", 1, false));

        PluginConfiguration config = PluginConfigurationMother.empty();

        String winner = PluginRegistry.findAll(SupportFilterType.class).values().stream()
                .filter(p -> p.supports(config))
                .min(Comparator.comparingInt(Plugin::priority))
                .map(p -> p.create(config))
                .orElseThrow();

        assertThat(winner).isEqualTo("SUPPORTED");
    }

    @Test
    void prioritySelection_noSupportingPlugins_noResult() {
        PluginRegistry.register(AllUnsupportedType.class, new AllUnsupportedType("NONE_A", 1));
        PluginRegistry.register(AllUnsupportedType.class, new AllUnsupportedType("NONE_B", 2));

        PluginConfiguration config = PluginConfigurationMother.empty();

        assertThatThrownBy(() ->
                PluginRegistry.findAll(AllUnsupportedType.class).values().stream()
                        .filter(p -> p.supports(config))
                        .min(Comparator.comparingInt(Plugin::priority))
                        .orElseThrow(IllegalStateException::new)
        ).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void hasAny_afterRegistration_returnsTrue() {
        PluginRegistry.register(HasAnyType.class, new HasAnyType("PRESENT"));
        assertThat(PluginRegistry.hasAny(HasAnyType.class)).isTrue();
    }
}
