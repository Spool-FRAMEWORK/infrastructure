package software.spool.infrastructure.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.exception.DuplicateEventException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.infrastructure.adapter.inbox.filesystem.FileSystemInboxEnvelopeRemover;
import software.spool.infrastructure.adapter.inbox.filesystem.FileSystemInboxReader;
import software.spool.infrastructure.adapter.inbox.filesystem.FileSystemInboxUpdater;
import software.spool.infrastructure.adapter.inbox.filesystem.FileSystemInboxWriter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FileSystemInboxIntegrationTest {

    @Test
    void reader_findByStatus_noDirectory_returnsEmpty(@TempDir Path tmp) throws Exception {
        FileSystemInboxReader reader = new FileSystemInboxReader(tmp.toString());
        assertThat(reader.findByStatus(EnvelopeStatus.CAPTURED)).isEmpty();
    }

    @Test
    void reader_findByStatus_emptyDirectory_returnsEmpty(@TempDir Path tmp) throws Exception {
        Files.createDirectories(tmp.resolve(EnvelopeStatus.CAPTURED.name()));
        FileSystemInboxReader reader = new FileSystemInboxReader(tmp.toString());
        assertThat(reader.findByStatus(EnvelopeStatus.CAPTURED)).isEmpty();
    }

    @Test
    void reader_findById_fileNotPresent_returnsEmpty(@TempDir Path tmp) throws Exception {
        FileSystemInboxReader reader = new FileSystemInboxReader(tmp.toString());
        assertThat(reader.findById(mockKey("non-existent-key"))).isEmpty();
    }

    @Test
    void reader_findByIds_allMissing_returnsEmptyCollection(@TempDir Path tmp) throws Exception {
        FileSystemInboxReader reader = new FileSystemInboxReader(tmp.toString());
        Collection<Envelope> result = reader.findByIds(List.of(mockKey("k1"), mockKey("k2")));
        assertThat(result).isEmpty();
    }

    @Test
    void writer_receive_duplicateKeyInCaptured_throwsDuplicateEventException(@TempDir Path tmp) throws Exception {
        String keyValue = "dup-in-captured";
        Path dir = tmp.resolve(EnvelopeStatus.CAPTURED.name());
        Files.createDirectories(dir);
        Files.write(dir.resolve(keyValue + ".json"), new byte[0]);

        assertThatThrownBy(() -> new FileSystemInboxWriter(tmp.toString()).receive(mockEnvelope(keyValue)))
                .isInstanceOf(DuplicateEventException.class);
    }

    @Test
    void writer_receive_duplicateKeyInIngested_throwsDuplicateEventException(@TempDir Path tmp) throws Exception {
        String keyValue = "dup-in-ingested";
        Path dir = tmp.resolve(EnvelopeStatus.INGESTED.name());
        Files.createDirectories(dir);
        Files.write(dir.resolve(keyValue + ".json"), new byte[0]);

        assertThatThrownBy(() -> new FileSystemInboxWriter(tmp.toString()).receive(mockEnvelope(keyValue)))
                .isInstanceOf(DuplicateEventException.class);
    }

    @Test
    void writer_receive_duplicateKeyInQuarantined_throwsDuplicateEventException(@TempDir Path tmp) throws Exception {
        String keyValue = "dup-in-quarantined";
        Path dir = tmp.resolve(EnvelopeStatus.QUARANTINED.name());
        Files.createDirectories(dir);
        Files.write(dir.resolve(keyValue + ".json"), new byte[0]);

        assertThatThrownBy(() -> new FileSystemInboxWriter(tmp.toString()).receive(mockEnvelope(keyValue)))
                .isInstanceOf(DuplicateEventException.class);
    }

    @Test
    void writer_receive_createsCapturedDirBeforeDuplicateCheck(@TempDir Path tmp) throws Exception {
        Path capturedDir = tmp.resolve(EnvelopeStatus.CAPTURED.name());
        assertThat(Files.exists(capturedDir)).isFalse();

        String keyValue = "dir-creation-key";
        Path ingestedDir = tmp.resolve(EnvelopeStatus.INGESTED.name());
        Files.createDirectories(ingestedDir);
        Files.write(ingestedDir.resolve(keyValue + ".json"), new byte[0]);

        assertThatThrownBy(() -> new FileSystemInboxWriter(tmp.toString()).receive(mockEnvelope(keyValue)))
                .isInstanceOf(DuplicateEventException.class);

        assertThat(Files.exists(capturedDir)).isTrue();
    }

    @Test
    void updater_update_keyNotFound_returnsEmptyCollection(@TempDir Path tmp) throws Exception {
        FileSystemInboxUpdater updater = new FileSystemInboxUpdater(tmp.toString());
        Collection<Envelope> result = updater.update(List.of(mockKey("missing")), EnvelopeStatus.INGESTED);
        assertThat(result).isEmpty();
    }

    @Test
    void updater_update_emptyKeys_returnsEmptyCollection(@TempDir Path tmp) throws Exception {
        FileSystemInboxUpdater updater = new FileSystemInboxUpdater(tmp.toString());
        assertThat(updater.update(List.of(), EnvelopeStatus.INGESTED)).isEmpty();
    }

    @Test
    void remover_remove_keyNotFound_returnsEmptyCollection(@TempDir Path tmp) {
        FileSystemInboxEnvelopeRemover remover = new FileSystemInboxEnvelopeRemover(tmp.toString());
        assertThat(remover.remove(List.of(mockKey("gone"), mockKey("also-gone")))).isEmpty();
    }

    @Test
    void remover_remove_emptyKeys_returnsEmptyCollection(@TempDir Path tmp) {
        FileSystemInboxEnvelopeRemover remover = new FileSystemInboxEnvelopeRemover(tmp.toString());
        assertThat(remover.remove(List.of())).isEmpty();
    }

    private static IdempotencyKey mockKey(String value) {
        IdempotencyKey key = mock(IdempotencyKey.class);
        when(key.value()).thenReturn(value);
        return key;
    }

    private static Envelope mockEnvelope(String keyValue) {
        IdempotencyKey key = mockKey(keyValue);
        Envelope envelope = mock(Envelope.class);
        when(envelope.idempotencyKey()).thenReturn(key);
        return envelope;
    }
}
