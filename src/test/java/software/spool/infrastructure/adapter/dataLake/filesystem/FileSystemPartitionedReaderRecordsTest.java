package software.spool.infrastructure.adapter.dataLake.filesystem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.infrastructure.adapter.datamart.FileSystemDataMartWriter;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemPartitionedReaderRecordsTest {
    private static final PartitionKey PARTITION = PartitionKey.ofEntriesWithoutDate().with("source", "payments").build();
    private static final String PAYLOAD = "{\"id\":\"payment-1\",\"amount\":10}";

    @TempDir
    Path dataLake;

    @Test
    void read_bronzeEnvelope_givesItsPayloadDecoded() {
        new FileSystemDataLakeWriter(dataLake.toString()).write(List.of(new PartitionedEnvelope(PARTITION, envelope()))).toList();

        List<PartitionedRecord<GenericRecord>> read = reader().read(MountTarget.transformation("payments", PARTITION)).toList();

        assertThat(read).singleElement().satisfies(record ->
                assertThat(record.record().getNested("payload").getString("id")).isEqualTo("payment-1"));
    }

    @Test
    void read_silverRecordWrittenByAMountWithItsPayloadDecoded_givesItsPayloadAsItIs() {
        new FileSystemDataMartWriter(dataLake).write(MountTarget.transformation("payments", PARTITION),
                Stream.of(new PartitionedRecord<>(PARTITION, silverRecord())));

        List<PartitionedRecord<GenericRecord>> read = reader().read(MountTarget.aggregation("totals", PARTITION)).toList();

        assertThat(read).singleElement().satisfies(record ->
                assertThat(record.record().getNested("payload").getString("id")).isEqualTo("payment-1"));
    }

    @Test
    void read_bronzeEnvelope_givesItsHeaderFieldsNextToItsPayload() {
        new FileSystemDataLakeWriter(dataLake.toString()).write(List.of(new PartitionedEnvelope(PARTITION, envelope()))).toList();

        List<PartitionedRecord<GenericRecord>> read = reader().read(MountTarget.transformation("payments", PARTITION)).toList();

        assertThat(read).singleElement().satisfies(record -> {
            assertThat(record.partitionKey()).isEqualTo(PARTITION);
            assertThat(record.record().getNested("idempotencyKey").getString("value")).isEqualTo("payment-1");
            assertThat(record.record().getNested("mediaType").getString("value")).isEqualTo("application/json");
        });
    }

    @Test
    void read_bronzeEnvelopeWithABinaryPayload_givesItsBytes() {
        byte[] pdf = {0x25, 0x50, 0x44, 0x46, (byte) 0xe2, (byte) 0xe3};
        Envelope binary = new Envelope(IdempotencyKey.of("report-1"), new EventMetadata().set(EventMetadataKey.SOURCE, "reports"),
                MediaType.of("application/pdf"), pdf, EnvelopeStatus.PERSISTED, 0, Instant.now(), Instant.now());
        new FileSystemDataLakeWriter(dataLake.toString()).write(List.of(new PartitionedEnvelope(PARTITION, binary))).toList();

        List<PartitionedRecord<GenericRecord>> read = reader().read(MountTarget.transformation("reports", PARTITION)).toList();

        assertThat(read).singleElement().satisfies(record -> assertThat(record.record().get("payload")).isEqualTo(pdf));
    }

    @Test
    void read_bronzeEnvelopeInTheFormerFormat_givesItsPayloadDecoded() throws Exception {
        Path folder = dataLake.resolve("bronze").resolve("source=payments");
        Files.createDirectories(folder);
        Files.write(folder.resolve("payment-1.json"), RecordSerializerFactory.record().serialize(envelope()));

        List<PartitionedRecord<GenericRecord>> read = reader().read(MountTarget.transformation("payments", PARTITION)).toList();

        assertThat(read).singleElement().satisfies(record ->
                assertThat(record.record().getNested("payload").getString("id")).isEqualTo("payment-1"));
    }

    private PartitionedReader reader() {
        return new FileSystemPartitionedReaderProvider().create(PluginConfiguration.builder().with("path", dataLake.toString()).build());
    }

    private static Envelope envelope() {
        return new Envelope(IdempotencyKey.of("payment-1"), new EventMetadata().set(EventMetadataKey.SOURCE, "payments"),
                MediaType.of("application/json"), PAYLOAD.getBytes(StandardCharsets.UTF_8), EnvelopeStatus.PERSISTED, 0,
                Instant.now(), Instant.now());
    }

    private static GenericRecord silverRecord() {
        Map<String, Object> fields = new LinkedHashMap<>(PayloadDeserializerFactory.json().asMap()
                .deserialize(RecordSerializerFactory.record().serialize(envelope())));
        fields.put("payload", PayloadDeserializerFactory.json().asMap().deserialize(PAYLOAD.getBytes(StandardCharsets.UTF_8)));
        return GenericRecord.of(fields);
    }
}
