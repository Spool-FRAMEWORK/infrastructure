package software.spool.infrastructure.adapter.dataLake.s3;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.infrastructure.adapter.datamart.S3DataMartWriter;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.port.scaling.PartitionSlice;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class S3PartitionedReaderTest {
    private static final String BUCKET = "spool-datalake-test";
    private static final PartitionKey YEAR = PartitionKey.ofEntriesWithoutDate().with("year", "2026").build();
    private static final PartitionKey PAYMENTS = PartitionKey.ofEntriesWithoutDate().with("year", "2026")
            .with("source", "payments").build();
    private static final PartitionKey REFUNDS = PartitionKey.ofEntriesWithoutDate().with("year", "2026")
            .with("source", "refunds").build();
    private static final PartitionKey SIBLING_YEAR = PartitionKey.ofEntriesWithoutDate().with("year", "20261").build();
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    private static final byte[] PDF = {0x25, 0x50, 0x44, 0x46, (byte) 0xe2, (byte) 0xe3};

    private final InMemoryS3Client s3Client = new InMemoryS3Client();
    private final BoundedConcurrency concurrency = BoundedConcurrency.withThreads(2);

    @AfterEach
    void closeConcurrency() {
        concurrency.close();
    }

    @Test
    void read_jsonEnvelope_givesItsPayloadAsAnObject() {
        write(PAYMENTS, json("payment-1", "payments"));

        List<PartitionedRecord<GenericRecord>> read = read(PAYMENTS);

        assertThat(read).singleElement().satisfies(record ->
                assertThat(record.record().getNested("payload").getString("id")).isEqualTo("payment-1"));
    }

    @Test
    void read_envelope_givesItsHeaderFieldsNextToItsPayload() {
        write(PAYMENTS, json("payment-1", "payments"));

        List<PartitionedRecord<GenericRecord>> read = read(PAYMENTS);

        assertThat(read).singleElement().satisfies(record -> {
            assertThat(record.partitionKey()).isEqualTo(PAYMENTS);
            assertThat(record.record().getNested("idempotencyKey").getString("value")).isEqualTo("payment-1");
            assertThat(record.record().getNested("mediaType").getString("value")).isEqualTo("application/json");
        });
    }

    @Test
    void read_binaryEnvelope_givesItsBytes() {
        write(PAYMENTS, pdf("report-1", "payments"));

        List<PartitionedRecord<GenericRecord>> read = read(PAYMENTS);

        assertThat(read).singleElement().satisfies(record -> assertThat(record.record().get("payload")).isEqualTo(PDF));
    }

    @Test
    void read_envelopeWhoseHeaderDidNotFitInTheMetadata_readsItFromItsMetaJson() {
        write(PAYMENTS, pdf("report-1", "x".repeat(3000)));

        List<PartitionedRecord<GenericRecord>> read = read(PAYMENTS);

        assertThat(s3Client.keys()).contains(S3DataLakeLayout.headerKeyOf(PAYMENTS, IdempotencyKey.of("report-1")));
        assertThat(read).singleElement().satisfies(record -> {
            assertThat(record.record().get("payload")).isEqualTo(PDF);
            assertThat(record.record().getNested("idempotencyKey").getString("value")).isEqualTo("report-1");
        });
    }

    @Test
    void read_envelopeInTheFormerFormat_givesItsPayloadDecoded() {
        Envelope former = json("payment-1", "payments");
        put(S3DataLakeLayout.formerKeyOf(PAYMENTS, former.idempotencyKey()), RecordSerializerFactory.record().serialize(former));

        List<PartitionedRecord<GenericRecord>> read = read(PAYMENTS);

        assertThat(read).singleElement().satisfies(record -> {
            assertThat(record.partitionKey()).isEqualTo(PAYMENTS);
            assertThat(record.record().getNested("payload").getString("id")).isEqualTo("payment-1");
        });
    }

    @Test
    void read_payloadWithoutHeader_isSkipped() {
        put("bronze/year=2026/source=payments/report-1.pdf", PDF);
        write(PAYMENTS, json("payment-1", "payments"));

        List<PartitionedRecord<GenericRecord>> read = read(PAYMENTS);

        assertThat(read).singleElement().satisfies(record ->
                assertThat(record.record().getNested("idempotencyKey").getString("value")).isEqualTo("payment-1"));
    }

    @Test
    void read_partition_alsoReadsThePartitionsBelowItWithTheirOwnPartition() {
        write(PAYMENTS, json("payment-1", "payments"));
        write(REFUNDS, json("refund-1", "refunds"));

        List<PartitionedRecord<GenericRecord>> read = read(YEAR);

        assertThat(read).extracting(PartitionedRecord::partitionKey).containsExactlyInAnyOrder(PAYMENTS, REFUNDS);
    }

    @Test
    void read_partition_doesNotReadASiblingPartitionThatSharesItsPrefix() {
        write(YEAR, json("payment-1", "payments"));
        write(SIBLING_YEAR, json("payment-2", "payments"));

        List<PartitionedRecord<GenericRecord>> read = read(YEAR);

        assertThat(read).extracting(PartitionedRecord::partitionKey).containsExactly(YEAR);
    }

    @Test
    void read_partitionKeptBehindTheDateEntries_readsItsEnvelopesWithTheirWholePartition() {
        PartitionKey dated = PartitionKey.ofEntriesWithoutDate().with("year", "2026").with("month", "11")
                .with("source", "payments").with("eventType", "Payment").build();
        write(dated, json("payment-1", "payments"));

        List<PartitionedRecord<GenericRecord>> read = read(PartitionKey.ofEntriesWithoutDate().with("source", "payments").build());

        assertThat(read).extracting(PartitionedRecord::partitionKey).containsExactly(dated);
    }

    @Test
    void read_partitionKeptBehindTheDateEntries_doesNotReadAPathThatLacksOneOfItsEntries() {
        PartitionKey payments = PartitionKey.ofEntriesWithoutDate().with("year", "2026").with("source", "payments")
                .with("eventType", "Payment").build();
        PartitionKey refunds = PartitionKey.ofEntriesWithoutDate().with("year", "2026").with("source", "payments")
                .with("eventType", "Refund").build();
        PartitionKey similarSource = PartitionKey.ofEntriesWithoutDate().with("year", "2026").with("source", "payments2")
                .with("eventType", "Payment").build();
        write(payments, json("payment-1", "payments"));
        write(refunds, json("refund-1", "payments"));
        write(similarSource, json("payment-2", "payments2"));

        List<PartitionedRecord<GenericRecord>> read = read(PartitionKey.ofEntriesWithoutDate().with("source", "payments")
                .with("eventType", "Payment").build());

        assertThat(read).extracting(PartitionedRecord::partitionKey).containsExactly(payments);
    }

    @Test
    void read_moreObjectsThanOneListingPage_readsThemAll() {
        int envelopes = 1001;
        writer().write(IntStream.range(0, envelopes)
                .mapToObj(index -> new PartitionedEnvelope(PAYMENTS, json("payment-" + index, "payments")))
                .toList()).toList();

        assertThat(read(PAYMENTS)).hasSize(envelopes);
    }

    @Test
    void read_slice_givesOnlyTheEnvelopesOfThatSlice() {
        write(PAYMENTS, json("payment-1", "payments"));
        write(PAYMENTS, json("payment-2", "payments"));
        write(PAYMENTS, json("payment-3", "payments"));

        List<PartitionedRecord<GenericRecord>> read = reader()
                .read(MountTarget.transformation("payments", PAYMENTS).withSlice(new PartitionSlice(PAYMENTS, 1, 1)))
                .toList();

        assertThat(read).singleElement().satisfies(record ->
                assertThat(record.record().getNested("idempotencyKey").getString("value")).isEqualTo("payment-2"));
    }

    @Test
    void read_aggregationTarget_givesTheSilverRecordsAMountWroteWithTheirPartition() {
        PartitionKey silverPartition = PartitionKey.ofEntriesWithoutDate().with("source", "payments")
                .with("eventType", "Payment").build();
        new S3DataMartWriter(s3Client, BUCKET, concurrency).write(MountTarget.transformation("payments", silverPartition),
                Stream.of(new PartitionedRecord<>(silverPartition, GenericRecord.of(Map.of("payload", Map.of("id", "payment-1"))))));

        List<PartitionedRecord<GenericRecord>> read = reader()
                .read(MountTarget.aggregation("totals", PartitionKey.ofEntriesWithoutDate().with("source", "payments").build()))
                .toList();

        assertThat(read).singleElement().satisfies(record -> {
            assertThat(record.partitionKey()).isEqualTo(new PartitionKey("payments::" + silverPartition.value()));
            assertThat(record.record().getNested("payload").getString("id")).isEqualTo("payment-1");
        });
    }

    @Test
    void read_aggregationTarget_doesNotReadBronze() {
        write(PAYMENTS, json("payment-1", "payments"));

        assertThat(reader().read(MountTarget.aggregation("totals", PAYMENTS)).toList()).isEmpty();
    }

    private List<PartitionedRecord<GenericRecord>> read(PartitionKey partition) {
        return reader().read(MountTarget.transformation("payments", partition)).toList();
    }

    private S3PartitionedReader reader() {
        return new S3PartitionedReader(s3Client, BUCKET, concurrency);
    }

    private S3DataLakeWriter writer() {
        return new S3DataLakeWriter(s3Client, BUCKET);
    }

    private void write(PartitionKey partition, Envelope envelope) {
        writer().write(List.of(new PartitionedEnvelope(partition, envelope))).toList();
    }

    private void put(String key, byte[] content) {
        s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET).key(key).build(), RequestBody.fromBytes(content));
    }

    private static Envelope json(String key, String source) {
        byte[] payload = ("{\"id\":\"" + key + "\",\"amount\":10}").getBytes(StandardCharsets.UTF_8);
        return envelope(key, source, MediaType.of("application/json"), payload);
    }

    private static Envelope pdf(String key, String source) {
        return envelope(key, source, MediaType.of("application/pdf"), PDF);
    }

    private static Envelope envelope(String key, String source, MediaType mediaType, byte[] payload) {
        return new Envelope(IdempotencyKey.of(key), new EventMetadata().set(EventMetadataKey.SOURCE, source), mediaType,
                payload, EnvelopeStatus.PERSISTED, 0, NOW, NOW);
    }
}
