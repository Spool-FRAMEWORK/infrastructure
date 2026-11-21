package software.spool.infrastructure.adapter.dataLake;

import software.spool.core.adapter.memory.InMemoryDataLake;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;
import software.spool.core.port.datalake.DataLakeWriter;

import java.util.Optional;

class InMemoryDataLakeContractTest extends DataLakeContract {

    private final InMemoryDataLake dataLake = new InMemoryDataLake();

    @Override
    protected DataLakeWriter writer() {
        return dataLake;
    }

    @Override
    protected DataLakeEnvelopeRemover remover() {
        return dataLake;
    }

    @Override
    protected Optional<Envelope> find(PartitionKey partition, IdempotencyKey idempotencyKey) {
        return dataLake.find(partition, idempotencyKey);
    }
}
