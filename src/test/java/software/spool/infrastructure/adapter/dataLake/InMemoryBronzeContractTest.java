package software.spool.infrastructure.adapter.dataLake;

import software.spool.core.adapter.memory.InMemoryBronze;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.BronzeEnvelopeRemover;
import software.spool.core.port.datalake.BronzeWriter;

import java.util.Optional;

class InMemoryBronzeContractTest extends BronzeContract {

    private final InMemoryBronze bronze = new InMemoryBronze();

    @Override
    protected BronzeWriter writer() {
        return bronze;
    }

    @Override
    protected BronzeEnvelopeRemover remover() {
        return bronze;
    }

    @Override
    protected Optional<Envelope> find(PartitionKey partition, IdempotencyKey idempotencyKey) {
        return bronze.find(partition, idempotencyKey);
    }
}
