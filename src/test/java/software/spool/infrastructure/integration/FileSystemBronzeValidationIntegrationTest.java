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
import software.spool.infrastructure.adapter.dataLake.filesystem.FileSystemBronzeRepository;
import software.spool.infrastructure.adapter.dataLake.filesystem.FileSystemPartitionedReaderProvider;
import software.spool.infrastructure.adapter.quarantine.FileSystemQuarantineRepository;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
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
        FileSystemBronzeRepository bronze = new FileSystemBronzeRepository(dataLake.toString());
        bronze.save(PARTITION, payment("payment-1", 10));
        bronze.save(PARTITION, payment("payment-2", -5));
        FileSystemQuarantineRepository quarantineRepository = new FileSystemQuarantineRepository(quarantine.toString());

        List<PartitionedRecord<GenericRecord>> promoted = validatingReader(bronze, quarantineRepository).read(TARGET).toList();

        assertThat(promoted).extracting(record -> record.record().getNested("payload").getString("id"))
                .containsExactly("payment-1");
        assertThat(quarantineRepository.find(IdempotencyKey.of("payment-2"))).isPresent();
        assertThat(bronzeFileNames()).containsExactly("payment-1.json");
    }

    private ValidatingPartitionedReader validatingReader(FileSystemBronzeRepository bronze,
                                                         FileSystemQuarantineRepository quarantineRepository) {
        return new ValidatingPartitionedReader(
                new FileSystemPartitionedReaderProvider().create(PluginConfiguration.builder().with("path", dataLake.toString()).build()),
                new EnvelopeValidator(new ValidatorRegistry(List.of(new NonNegativeAmountValidator()))),
                quarantineRepository,
                bronze,
                new EventPublisher() {
                    @Override
                    public <E extends Event> void publish(E event) {
                    }
                },
                MetricsRegistry.NOOP.counter("rejected", "rejected", "1"));
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
