package software.spool.infrastructure.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.Event;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.metrics.MetricsRegistry;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.infrastructure.adapter.dataLake.filesystem.FileSystemDataLakeEnvelopeRemover;
import software.spool.infrastructure.adapter.dataLake.filesystem.FileSystemDataLakeWriter;
import software.spool.infrastructure.adapter.dataLake.filesystem.FileSystemPartitionedReaderProvider;
import software.spool.infrastructure.adapter.quarantine.FileSystemQuarantineRepository;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.internal.quarantine.DataLakeRecordQuarantine;
import software.spool.mounter.internal.validation.ValidatingPartitionedReader;
import software.spool.validator.api.EnvelopeValidator;
import software.spool.validator.api.Validate;
import software.spool.validator.api.Validator;
import software.spool.validator.engine.ValidatorRegistry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemBronzeValidationIntegrationTest {

    private static final PartitionKey PARTITION = new PartitionKey("source=payments/eventType=Payment");
    private static final MountTarget TARGET = MountTarget.transformation("payments", PARTITION);

    public record Payment(String id, long amount) {
    }

    @Validate(value = Payment.class, sourceId = "payments")
    public static class NonNegativeAmountValidator implements Validator<Payment> {
        @Override
        public boolean validate(Payment payment) {
            return payment.amount() >= 0;
        }
    }

    @TempDir
    Path dataLake;

    @TempDir
    Path quarantine;

    @Test
    void read_bronzeOfTheFileSystem_passesTheValidPaymentAndQuarantinesTheOther() throws IOException {
        new FileSystemDataLakeWriter(dataLake.toString()).write(List.of(new PartitionedEnvelope(PARTITION, payment("payment-1", 10)),
                new PartitionedEnvelope(PARTITION, payment("payment-2", -5)))).toList();
        FileSystemQuarantineRepository quarantineRepository = new FileSystemQuarantineRepository(quarantine.toString());

        List<PartitionedRecord<GenericRecord>> promoted = validatingReader(quarantineRepository).read(TARGET).toList();

        assertThat(promoted).extracting(record -> record.record().getNested("payload").getString("id"))
                .containsExactly("payment-1");
        assertThat(quarantineRepository.find(IdempotencyKey.of("payment-2"))).isPresent();
        assertThat(bronzeFileNames()).containsExactlyInAnyOrder("payment-1.json", "payment-1.meta.json");
    }

    private ValidatingPartitionedReader validatingReader(FileSystemQuarantineRepository quarantineRepository) {
        return new ValidatingPartitionedReader(
                new FileSystemPartitionedReaderProvider().create(PluginConfiguration.builder().with("path", dataLake.toString()).build()),
                new EnvelopeValidator(new ValidatorRegistry(List.of(new NonNegativeAmountValidator()))),
                new DataLakeRecordQuarantine(quarantineRepository,
                        new FileSystemDataLakeEnvelopeRemover(dataLake.toString()),
                        new EventPublisher() {
                            @Override
                            public <E extends Event> void publish(E event) {
                            }
                        },
                        MetricsRegistry.NOOP.counter("rejected", "rejected", "1")));
    }

    private List<String> bronzeFileNames() throws IOException {
        try (Stream<Path> walk = Files.walk(dataLake.resolve("bronze"))) {
            return walk.filter(Files::isRegularFile).map(file -> file.getFileName().toString()).toList();
        }
    }

    private static Envelope payment(String id, long amount) {
        byte[] payload = ("{\"id\":\"" + id + "\",\"amount\":" + amount + "}").getBytes(StandardCharsets.UTF_8);
        return new Envelope(IdempotencyKey.of(id), new EventMetadata().set(EventMetadataKey.SOURCE, "payments"),
                MediaType.of("application/json"), payload, EnvelopeStatus.PERSISTED, 0, Instant.now(), Instant.now());
    }
}
