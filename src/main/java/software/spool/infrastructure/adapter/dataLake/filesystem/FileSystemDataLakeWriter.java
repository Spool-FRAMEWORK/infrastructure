package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.core.port.datalake.DataLakeWriter;
import software.spool.core.port.serde.RecordSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * {@link DataLakeWriter} that keeps the payload of each envelope as it came and its header next to it, laid out by
 * {@link FileSystemDataLakeLayout}. The header is written last: an envelope is there once its header is.
 */
public class FileSystemDataLakeWriter implements DataLakeWriter {
    private final FileSystemDataLakeLayout layout;
    private final RecordSerializer<EnvelopeHeader> serializer = RecordSerializerFactory.record();

    public FileSystemDataLakeWriter(String path) {
        this.layout = new FileSystemDataLakeLayout(path);
    }

    @Override
    public Stream<IdempotencyKey> write(Collection<PartitionedEnvelope> items) {
        return items.stream()
                .map(this::writeEnvelope)
                .filter(Objects::nonNull);
    }

    private IdempotencyKey writeEnvelope(PartitionedEnvelope item) {
        Envelope envelope = item.envelope();
        Path headerFile = layout.headerFileOf(item.partition(), envelope.idempotencyKey());
        try {
            Files.createDirectories(headerFile.getParent());
            Files.write(layout.payloadFileOf(item.partition(), envelope.header()), envelope.payload());
            Files.write(headerFile, serializer.serialize(envelope.header()));
            return envelope.idempotencyKey();
        } catch (IOException e) {
            return null;
        }
    }
}
