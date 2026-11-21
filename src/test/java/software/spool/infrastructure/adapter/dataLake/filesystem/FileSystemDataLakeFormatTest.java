package software.spool.infrastructure.adapter.dataLake.filesystem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.model.vo.PartitionedEnvelope;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemDataLakeFormatTest {
    private static final PartitionKey PARTITION = PartitionKey.ofEntriesWithoutDate().with("source", "boe").build();
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    private static final byte[] PDF = {0x25, 0x50, 0x44, 0x46, 0x2d, 0x31, 0x2e, 0x37, (byte) 0xe2, (byte) 0xe3};

    @TempDir
    Path dataLake;

    @Test
    void write_jsonPayload_keepsItAsItCameNextToItsHeader() throws IOException {
        Envelope envelope = envelope("k1", MediaType.of("application/json"), "{\"price\":10}".getBytes(StandardCharsets.UTF_8));

        write(envelope);

        assertThat(Files.readString(partitionFolder().resolve("k1.json"))).isEqualTo("{\"price\":10}");
        assertThat(headerIn(partitionFolder().resolve("k1.meta.json"))).isEqualTo(envelope.header());
    }

    @Test
    void write_pdfPayload_keepsItsBytesInAPdfFile() throws IOException {
        Envelope envelope = envelope("k2", MediaType.of("application/pdf"), PDF);

        write(envelope);

        assertThat(Files.readAllBytes(partitionFolder().resolve("k2.pdf"))).isEqualTo(PDF);
        assertThat(headerIn(partitionFolder().resolve("k2.meta.json"))).isEqualTo(envelope.header());
    }

    @Test
    void write_payloadOfAJsonBasedMediaType_keepsItInAJsonFile() throws IOException {
        write(envelope("k3", MediaType.of("application/ld+json"), "{\"@id\":\"x\"}".getBytes(StandardCharsets.UTF_8)));

        assertThat(partitionFolder().resolve("k3.json")).exists();
    }

    @Test
    void remove_envelope_removesItsPayloadAndItsHeader() throws IOException {
        write(envelope("k2", MediaType.of("application/pdf"), PDF));

        new FileSystemDataLakeEnvelopeRemover(dataLake.toString()).remove(PARTITION, IdempotencyKey.of("k2"));

        assertThat(filesIn(partitionFolder())).isEmpty();
    }

    @Test
    void remove_envelopeInTheFormerFormat_removesItsFile() throws IOException {
        Files.createDirectories(partitionFolder());
        Files.writeString(partitionFolder().resolve("k1.json"), "{\"idempotencyKey\":{\"value\":\"k1\"},\"payload\":\"e30=\"}");

        new FileSystemDataLakeEnvelopeRemover(dataLake.toString()).remove(PARTITION, IdempotencyKey.of("k1"));

        assertThat(filesIn(partitionFolder())).isEmpty();
    }

    private void write(Envelope envelope) {
        new FileSystemDataLakeWriter(dataLake.toString()).write(List.of(new PartitionedEnvelope(PARTITION, envelope))).toList();
    }

    private Path partitionFolder() {
        return dataLake.resolve("bronze").resolve("source=boe");
    }

    private static EnvelopeHeader headerIn(Path file) throws IOException {
        return PayloadDeserializerFactory.json().as(EnvelopeHeader.class).deserialize(Files.readAllBytes(file));
    }

    private static List<String> filesIn(Path folder) throws IOException {
        try (Stream<Path> files = Files.list(folder)) {
            return files.map(file -> file.getFileName().toString()).toList();
        }
    }

    private static Envelope envelope(String key, MediaType mediaType, byte[] payload) {
        return new Envelope(IdempotencyKey.of(key), new EventMetadata().set(EventMetadataKey.SOURCE, "boe"), mediaType,
                payload, EnvelopeStatus.PERSISTED, 0, NOW, NOW);
    }
}
