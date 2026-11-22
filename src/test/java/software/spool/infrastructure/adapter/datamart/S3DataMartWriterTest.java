package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.PartitionKey;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class S3DataMartWriterTest {
    private static final String BUCKET = "spool-datamart-test";
    private static final PartitionKey PARTITION = PartitionKey.ofEntriesWithoutDate().with("source", "payments")
            .with("eventType", "Payment").build();

    private final InMemoryS3Client s3Client = new InMemoryS3Client();
    private final BoundedConcurrency concurrency = BoundedConcurrency.withThreads(2);

    @AfterEach
    void closeConcurrency() {
        concurrency.close();
    }

    @Test
    void write_transformationResult_isAJsonObjectInSilverUnderItsDataMartAndPartition() {
        writer().write(MountTarget.transformation("payments", PARTITION), Stream.of(result("payment-1")));

        assertThat(s3Client.keys()).singleElement().satisfies(key -> {
            assertThat(key).startsWith("silver/payments/source=payments/eventType=Payment/").endsWith(".json");
            assertThat(contentOf(key)).containsEntry("id", "payment-1");
        });
    }

    @Test
    void write_aggregationResult_goesToGold() {
        writer().write(MountTarget.aggregation("totals", PARTITION), Stream.of(result("total-1")));

        assertThat(s3Client.keys()).singleElement().satisfies(key ->
                assertThat(key).startsWith("gold/totals/source=payments/eventType=Payment/"));
    }

    @Test
    void write_sameResultTwice_keepsASingleObject() {
        writer().write(MountTarget.transformation("payments", PARTITION), Stream.of(result("payment-1")));
        writer().write(MountTarget.transformation("payments", PARTITION), Stream.of(result("payment-1")));

        assertThat(s3Client.keys()).hasSize(1);
    }

    @Test
    void write_severalResults_writesOneObjectForEach() {
        writer().write(MountTarget.transformation("payments", PARTITION),
                Stream.of(result("payment-1"), result("payment-2"), result("payment-3")));

        assertThat(s3Client.keys()).hasSize(3);
    }

    @Test
    void write_resultWithABinaryPayload_keepsThePayloadAsItIsAndTheRestOfTheRecordNextToIt() {
        writer().write(MountTarget.transformation("reports", PARTITION),
                Stream.of(new PartitionedRecord<>(PARTITION, BinaryResults.pdfRecord())));

        assertThat(s3Client.keys()).hasSize(2);
        String payload = s3Client.keys().stream().filter(key -> key.endsWith(".pdf")).findFirst().orElseThrow();
        assertThat(bytesOf(payload)).isEqualTo(BinaryResults.PDF);
        assertThat(contentOf(payload.replace(".pdf", ".meta.json")))
                .doesNotContainKey("payload")
                .containsEntry("mediaType", Map.of("value", "application/pdf"));
    }

    private S3DataMartWriter writer() {
        return new S3DataMartWriter(s3Client, BUCKET, concurrency);
    }

    private Map<String, Object> contentOf(String key) {
        return PayloadDeserializerFactory.json().asMap().deserialize(bytesOf(key));
    }

    private byte[] bytesOf(String key) {
        return s3Client.getObjectAsBytes(GetObjectRequest.builder().bucket(BUCKET).key(key).build()).asByteArray();
    }

    private static PartitionedRecord<?> result(String id) {
        return new PartitionedRecord<>(PARTITION, GenericRecord.of(Map.of("id", id, "amount", 10)));
    }
}
