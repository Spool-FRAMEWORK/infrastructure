package software.spool.infrastructure.adapter.dataLake.filesystem;

import org.junit.jupiter.api.io.TempDir;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;
import software.spool.core.port.datalake.DataLakeWriter;
import software.spool.infrastructure.adapter.dataLake.DataLakeContract;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

class FileSystemDataLakeContractTest extends DataLakeContract {

    @TempDir
    Path directory;

    @Override
    protected DataLakeWriter writer() {
        return new FileSystemDataLakeWriter(directory.toString());
    }

    @Override
    protected DataLakeEnvelopeRemover remover() {
        return new FileSystemDataLakeEnvelopeRemover(directory.toString());
    }

    @Override
    protected Optional<Envelope> find(PartitionKey partition, IdempotencyKey idempotencyKey) throws Exception {
        FileSystemDataLakeLayout layout = new FileSystemDataLakeLayout(directory.toString());
        Path headerFile = layout.headerFileOf(partition, idempotencyKey);
        if (!Files.exists(headerFile)) return Optional.empty();
        EnvelopeHeader header = PayloadDeserializerFactory.json().as(EnvelopeHeader.class).deserialize(Files.readAllBytes(headerFile));
        return Optional.of(Envelope.of(header, Files.readAllBytes(layout.payloadFileOf(partition, header))));
    }
}
