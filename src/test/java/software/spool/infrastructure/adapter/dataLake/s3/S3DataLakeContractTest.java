package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;
import software.spool.core.port.datalake.DataLakeWriter;
import software.spool.infrastructure.adapter.dataLake.DataLakeContract;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;

import java.util.Optional;

class S3DataLakeContractTest extends DataLakeContract {

    private static final String BUCKET = "spool-datalake-test";

    private final InMemoryS3Client s3Client = new InMemoryS3Client();

    @Override
    protected DataLakeWriter writer() {
        return new S3DataLakeWriter(s3Client, BUCKET);
    }

    @Override
    protected DataLakeEnvelopeRemover remover() {
        return new S3DataLakeEnvelopeRemover(s3Client, BUCKET);
    }

    @Override
    protected Optional<Envelope> find(PartitionKey partition, IdempotencyKey idempotencyKey) {
        return s3Client.keys().stream()
                .filter(key -> key.startsWith(S3DataLakeLayout.prefixOf(partition, idempotencyKey)))
                .filter(key -> !key.equals(S3DataLakeLayout.headerKeyOf(partition, idempotencyKey)))
                .findFirst()
                .map(payloadKey -> {
                    ResponseBytes<GetObjectResponse> payload = get(payloadKey);
                    EnvelopeHeader header = S3EnvelopeHeaderMetadata.headerIn(payload.response().metadata())
                            .orElseGet(() -> PayloadDeserializerFactory.json().as(EnvelopeHeader.class)
                                    .deserialize(get(S3DataLakeLayout.headerKeyOf(partition, idempotencyKey)).asByteArray()));
                    return Envelope.of(header, payload.asByteArray());
                });
    }

    private ResponseBytes<GetObjectResponse> get(String key) {
        return s3Client.getObjectAsBytes(GetObjectRequest.builder().bucket(BUCKET).key(key).build());
    }
}
