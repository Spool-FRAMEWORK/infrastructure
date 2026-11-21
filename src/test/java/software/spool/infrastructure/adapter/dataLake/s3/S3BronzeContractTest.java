package software.spool.infrastructure.adapter.dataLake.s3;

import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.BronzeEnvelopeRemover;
import software.spool.core.port.datalake.BronzeWriter;
import software.spool.infrastructure.adapter.dataLake.BronzeContract;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;

import java.util.Optional;

class S3BronzeContractTest extends BronzeContract {

    private static final String BUCKET = "spool-datalake-test";

    private final InMemoryS3Client s3Client = new InMemoryS3Client();

    @Override
    protected BronzeWriter writer() {
        return new S3BronzeWriter(s3Client, BUCKET);
    }

    @Override
    protected BronzeEnvelopeRemover remover() {
        return new S3BronzeEnvelopeRemover(s3Client, BUCKET);
    }

    @Override
    protected Optional<Envelope> find(PartitionKey partition, IdempotencyKey idempotencyKey) {
        String key = "bronze/" + String.join("/", KeyEntries.split(partition.value())) + "/" + idempotencyKey.value();
        try {
            byte[] content = s3Client.getObjectAsBytes(GetObjectRequest.builder().bucket(BUCKET).key(key).build()).asByteArray();
            return Optional.of(PayloadDeserializerFactory.json().as(Envelope.class).deserialize(content));
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        }
    }
}
