package software.spool.infrastructure.adapter.dataLake.s3;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
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
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class S3DataLakeFormatTest {
    private static final String BUCKET = "spool-datalake-test";
    private static final PartitionKey PARTITION = PartitionKey.ofEntriesWithoutDate().with("source", "boe").build();
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    private static final byte[] PDF = {0x25, 0x50, 0x44, 0x46, 0x2d, 0x31, 0x2e, 0x37, (byte) 0xe2, (byte) 0xe3};
    private static final int MORE_THAN_S3_ALLOWS_IN_METADATA = 3000;

    private final InMemoryS3Client s3Client = new InMemoryS3Client();

    @Test
    void write_jsonPayload_isOneObjectWithThePayloadAsItCameItsContentTypeAndItsHeader() {
        Envelope envelope = envelope("k1", MediaType.of("application/json"), "{\"price\":10}".getBytes(StandardCharsets.UTF_8), "boe");

        write(envelope);

        ResponseBytes<GetObjectResponse> object = get("bronze/source=boe/k1.json");
        assertThat(object.asUtf8String()).isEqualTo("{\"price\":10}");
        assertThat(object.response().contentType()).isEqualTo("application/json");
        assertThat(S3EnvelopeHeaderMetadata.headerIn(object.response().metadata())).contains(envelope.header());
        assertThat(s3Client.requests(InMemoryS3Client.PUT)).isEqualTo(1);
        assertThat(s3Client.keys()).containsExactly("bronze/source=boe/k1.json");
    }

    @Test
    void write_pdfPayload_keepsItsBytesInAPdfObject() {
        write(envelope("k2", MediaType.of("application/pdf"), PDF, "boe"));

        ResponseBytes<GetObjectResponse> object = get("bronze/source=boe/k2.pdf");
        assertThat(object.asByteArray()).isEqualTo(PDF);
        assertThat(object.response().contentType()).isEqualTo("application/pdf");
    }

    @Test
    void write_headerThatDoesNotFitInTheObjectMetadata_keepsItInAnObjectNextToThePayload() {
        Envelope envelope = envelope("k3", MediaType.of("application/json"), "{}".getBytes(StandardCharsets.UTF_8),
                "x".repeat(MORE_THAN_S3_ALLOWS_IN_METADATA));

        write(envelope);

        assertThat(s3Client.keys()).containsExactlyInAnyOrder("bronze/source=boe/k3.json", "bronze/source=boe/k3.meta.json");
        assertThat(get("bronze/source=boe/k3.json").response().metadata()).isEmpty();
        assertThat(PayloadDeserializerFactory.json().as(EnvelopeHeader.class)
                .deserialize(get("bronze/source=boe/k3.meta.json").asByteArray())).isEqualTo(envelope.header());
    }

    @Test
    void remove_envelope_removesItsPayloadAndItsHeader() {
        write(envelope("k3", MediaType.of("application/json"), "{}".getBytes(StandardCharsets.UTF_8),
                "x".repeat(MORE_THAN_S3_ALLOWS_IN_METADATA)));
        write(envelope("k4", MediaType.of("application/pdf"), PDF, "boe"));

        S3DataLakeEnvelopeRemover remover = new S3DataLakeEnvelopeRemover(s3Client, BUCKET);
        remover.remove(PARTITION, IdempotencyKey.of("k3"));
        remover.remove(PARTITION, IdempotencyKey.of("k4"));

        assertThat(s3Client.keys()).isEmpty();
    }

    @Test
    void remove_envelopeInTheFormerFormat_removesItsObject() {
        s3Client.put("bronze/source=boe/k1");

        new S3DataLakeEnvelopeRemover(s3Client, BUCKET).remove(PARTITION, IdempotencyKey.of("k1"));

        assertThat(s3Client.keys()).isEmpty();
    }

    private void write(Envelope envelope) {
        new S3DataLakeWriter(s3Client, BUCKET).write(List.of(new PartitionedEnvelope(PARTITION, envelope))).toList();
    }

    private ResponseBytes<GetObjectResponse> get(String key) {
        return s3Client.getObjectAsBytes(GetObjectRequest.builder().bucket(BUCKET).key(key).build());
    }

    private static Envelope envelope(String key, MediaType mediaType, byte[] payload, String source) {
        return new Envelope(IdempotencyKey.of(key), new EventMetadata().set(EventMetadataKey.SOURCE, source), mediaType,
                payload, EnvelopeStatus.PERSISTED, 0, NOW, NOW);
    }
}
