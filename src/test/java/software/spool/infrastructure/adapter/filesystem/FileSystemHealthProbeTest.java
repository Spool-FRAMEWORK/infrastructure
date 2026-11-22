package software.spool.infrastructure.adapter.filesystem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.port.health.HealthCheck;
import software.spool.core.port.health.HealthStatus;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemHealthProbeTest {
    @TempDir
    Path folder;

    @Test
    void probe_folderThatCanBeWritten_isHealthyAndLeavesNothingBehind() throws IOException {
        HealthCheck check = new FileSystemHealthProbe(folder).probe();

        assertThat(check.status()).isEqualTo(HealthStatus.HEALTHY);
        try (var files = Files.list(folder)) {
            assertThat(files).isEmpty();
        }
    }

    @Test
    void probe_folderThatDoesNotExist_isUnhealthyAndNamesTheFolder() {
        Path missing = folder.resolve("missing");

        HealthCheck check = new FileSystemHealthProbe(missing).probe();

        assertThat(check.status()).isEqualTo(HealthStatus.UNHEALTHY);
        assertThat(check.reason()).contains(missing.toString()).contains("does not exist");
    }

    @Test
    void probe_pathThatIsAFile_isUnhealthy() throws IOException {
        Path file = Files.createFile(folder.resolve("lake"));

        HealthCheck check = new FileSystemHealthProbe(file).probe();

        assertThat(check.status()).isEqualTo(HealthStatus.UNHEALTHY);
        assertThat(check.reason()).contains(file.toString());
    }

    @Test
    void provider_buildsTheCheckOfThePathOfThePiece() {
        FileSystemHealthProbeProvider provider = new FileSystemHealthProbeProvider();
        PluginConfiguration configuration = PluginConfiguration.builder().with("path", folder.toString()).build();

        assertThat(provider.name()).isEqualTo("FILE_SYSTEM");
        assertThat(provider.supports(configuration)).isTrue();
        assertThat(provider.create(configuration).probe().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    void rawProvider_goesWithTheRawFileSystemType() {
        assertThat(new RawFileSystemHealthProbeProvider().name()).isEqualTo("RAW_FILE_SYSTEM");
    }
}
