package software.spool.infrastructure.adapter.dataLake;

import software.spool.core.adapter.memory.InMemoryBronzeRepository;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.BronzeRepository;

import java.util.Optional;

class InMemoryBronzeRepositoryContractTest extends BronzeRepositoryContract {

    private final InMemoryBronzeRepository repository = new InMemoryBronzeRepository();

    @Override
    protected BronzeRepository repository() {
        return repository;
    }

    @Override
    protected Optional<Envelope> find(PartitionKey partition, IdempotencyKey idempotencyKey) {
        return repository.find(partition, idempotencyKey);
    }
}
