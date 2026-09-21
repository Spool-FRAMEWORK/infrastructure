package software.spool.infrastructure.scan;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.infrastructure.PluginConflictException;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.fixture.conflict.ConflictPluginOne;
import software.spool.infrastructure.fixture.conflict.ConflictPluginTwo;
import software.spool.infrastructure.fixture.conflict.ConflictPort;
import software.spool.infrastructure.fixture.conflict.CustomStylePlugin;
import software.spool.infrastructure.fixture.conflict.OverridePort;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PluginConflictScanTest {

    @Test
    void get_twoPluginsWithTheSameNameAndPriority_throwsNamingBothClasses() {
        assertThatThrownBy(() -> PluginResolver.get(ConflictPort.class, "CLASH"))
                .isInstanceOf(PluginConflictException.class)
                .hasMessageContaining("CLASH")
                .hasMessageContaining("ConflictPluginOne")
                .hasMessageContaining("ConflictPluginTwo");
    }

    @Test
    void get_afterAConflict_failsAgainInsteadOfWorkingWithAHalfLoadedRegistry() {
        assertThatThrownBy(() -> PluginResolver.get(ConflictPort.class, "CLASH"))
                .isInstanceOf(PluginConflictException.class);
        assertThatThrownBy(() -> PluginResolver.get(ConflictPort.class, "CLASH"))
                .isInstanceOf(PluginConflictException.class);
    }

    @Test
    void get_aProviderOfYourOwnWithALowerPriorityNumber_winsOverTheFrameworkOne() {
        OverridePort resolved = PluginResolver.get(OverridePort.class, "S3");

        assertThat(resolved).isInstanceOf(CustomStylePlugin.class);
    }

    @Test
    void scan_aJarWithTwoPluginsThatClash_throwsInsteadOfIgnoringIt(@TempDir Path directory) throws IOException {
        Path jar = directory.resolve("plugins.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            addClass(output, ConflictPluginOne.class);
            addClass(output, ConflictPluginTwo.class);
        }

        assertThatThrownBy(() -> new JarScanner().scan(jar.toFile(), ConflictPort.class, getClass().getClassLoader()))
                .isInstanceOf(PluginConflictException.class);
    }

    private static void addClass(JarOutputStream output, Class<?> type) throws IOException {
        String entry = type.getName().replace('.', '/') + ".class";
        output.putNextEntry(new JarEntry(entry));
        try (InputStream input = type.getClassLoader().getResourceAsStream(entry)) {
            input.transferTo(output);
        }
        output.closeEntry();
    }
}
