package software.spool.infrastructure.adapter.dataLake.filesystem;

import org.junit.jupiter.api.io.TempDir;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.BronzeRepository;
import software.spool.infrastructure.adapter.dataLake.BronzeRepositoryContract;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

class FileSystemBronzeRepositoryContractTest extends BronzeRepositoryContract {

    @TempDir
    Path directory;

    @Override
    protected BronzeRepository repository() {
        return new FileSystemBronzeRepository(directory.toString());
    }

    @Override
    protected Optional<Envelope> find(PartitionKey partition, IdempotencyKey idempotencyKey) throws Exception {
        Path file = directory.resolve("bronze").resolve(String.join("/", KeyEntries.split(partition.value())))
                .resolve(idempotencyKey.value() + ".json");
        if (!Files.exists(file)) return Optional.empty();
        return Optional.of(PayloadDeserializerFactory.json().as(Envelope.class).deserialize(Files.readAllBytes(file)));
    }
}
