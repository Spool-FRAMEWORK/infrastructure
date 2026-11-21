package software.spool.infrastructure.adapter.quarantine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.quarantine.QuarantineLocation;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.quarantine.QuarantineResolution;
import software.spool.core.model.quarantine.Resolver;
import software.spool.core.model.quarantine.QuarantinedRecord;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.core.utils.media.MediaTypes;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemQuarantineRepositoryContractTest extends QuarantineRepositoryContract {

    @TempDir
    Path directory;

    private QuarantineRepository repository;

    @BeforeEach
    void createRepository() {
        repository = new FileSystemQuarantineRepository(quarantineDirectory().toString());
    }

    @Override
    protected QuarantineRepository repository() {
        return repository;
    }

    @Test
    void save_resolvingARecord_movesItsFileToTheFolderOfItsStatus() {
        Instant now = Instant.now();
        QuarantinedRecord pending = QuarantinedRecord.pending(new Envelope(IdempotencyKey.of("k1"), new EventMetadata(),
                        MediaTypes.JSON, "{}".getBytes(StandardCharsets.UTF_8), EnvelopeStatus.CAPTURED, 0, now, now),
                QuarantineLocation.inbox(), QuarantineReason.VALIDATION_FAILED, List.of(), now);
        repository.save(pending);

        repository.save(pending.resolve(QuarantineResolution.republished(now, Resolver.unknown())));

        assertThat(quarantineDirectory().resolve("PENDING").resolve("k1.json")).doesNotExist();
        assertThat(quarantineDirectory().resolve("REPUBLISHED").resolve("k1.json")).exists();
    }

    private Path quarantineDirectory() {
        return directory.resolve("quarantine");
    }
}
