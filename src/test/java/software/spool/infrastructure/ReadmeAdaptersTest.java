package software.spool.infrastructure;

import org.junit.jupiter.api.Test;
import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.provider.ErrorRouterProvider;
import software.spool.infrastructure.spi.provider.PollSourceProvider;
import software.spool.infrastructure.spi.provider.StreamSourceProvider;
import software.spool.infrastructure.spi.provider.authentication.RequestAuthenticatorProvider;
import software.spool.infrastructure.spi.provider.provisioning.ProvisioningProvider;
import software.spool.infrastructure.spi.provider.health.HealthProbeProvider;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.dataLake.DataLakeEnvelopeRemoverProvider;
import software.spool.infrastructure.spi.provider.dataLake.DataLakeWriterProvider;
import software.spool.infrastructure.spi.provider.dataLake.PartitionedReaderProvider;
import software.spool.infrastructure.spi.provider.datamart.DataMartWriterProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxEnvelopeRemoverProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxReaderProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxUpdaterProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxWriterProvider;
import software.spool.infrastructure.spi.provider.quarantine.QuarantineRepositoryProvider;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The table of adapters in the README is the catalogue of the plugins: every port is in it, and each row lists
 * exactly the types registered for that port. A plugin added without documenting it, or a type documented
 * that does not exist, fails here.
 */
class ReadmeAdaptersTest {
    private static final Path README = Path.of("README.MD");
    private static final Pattern ROW = Pattern.compile("\\|[^|]*\\|\\s*`(\\w+)`\\s*\\|([^|]*)\\|[^|]*\\|");
    private static final Pattern TYPE = Pattern.compile("`(\\w+)`");
    private static final String NORMALIZER_SUFFIX = "_NORMALIZER";

    private static final List<Class<? extends Plugin<?>>> PORTS = List.of(
            EventBusProvider.class,
            InboxWriterProvider.class, InboxReaderProvider.class, InboxUpdaterProvider.class,
            InboxEnvelopeRemoverProvider.class,
            DataLakeWriterProvider.class, DataLakeEnvelopeRemoverProvider.class, PartitionedReaderProvider.class,
            DataMartWriterProvider.class,
            QuarantineRepositoryProvider.class,
            PollSourceProvider.class, StreamSourceProvider.class,
            NormalizerProvider.class,
            ErrorRouterProvider.class,
            ProvisioningProvider.class,
            HealthProbeProvider.class,
            RequestAuthenticatorProvider.class);

    @Test
    void theTableListsExactlyTheTypesRegisteredForEveryPort() throws IOException {
        Map<String, Set<String>> documented = documentedTypes();

        for (Class<? extends Plugin<?>> port : PORTS) {
            assertThat(documented).as("the README table has a row for " + port.getSimpleName())
                    .containsKey(port.getSimpleName());
            assertThat(documented.get(port.getSimpleName())).as("types of " + port.getSimpleName())
                    .containsExactlyInAnyOrderElementsOf(registeredTypes(port));
        }
        assertThat(documented.keySet()).as("rows of the README table")
                .containsExactlyInAnyOrderElementsOf(PORTS.stream().map(Class::getSimpleName).toList());
    }

    private static Map<String, Set<String>> documentedTypes() throws IOException {
        String section = Files.readString(README).split("## Available Adapters")[1].split("\\R## ")[0];
        Map<String, Set<String>> types = new LinkedHashMap<>();
        for (String line : section.lines().toList()) {
            Matcher row = ROW.matcher(line);
            if (!row.matches()) continue;
            Matcher type = TYPE.matcher(row.group(2));
            types.put(row.group(1), type.results().map(found -> found.group(1)).collect(Collectors.toSet()));
        }
        return types;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Set<String> registeredTypes(Class<? extends Plugin<?>> port) {
        Set<String> names = PluginResolver.available((Class) port).keySet();
        return names.stream()
                .map(name -> port == NormalizerProvider.class ? name.replaceFirst(NORMALIZER_SUFFIX + "$", "") : name)
                .collect(Collectors.toSet());
    }
}
