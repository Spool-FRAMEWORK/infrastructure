package software.spool.infrastructure.adapter.quarantine;

import software.spool.core.adapter.memory.InMemoryQuarantineRepository;
import software.spool.core.port.quarantine.QuarantineRepository;

class InMemoryQuarantineRepositoryContractTest extends QuarantineRepositoryContract {

    private final QuarantineRepository repository = new InMemoryQuarantineRepository();

    @Override
    protected QuarantineRepository repository() {
        return repository;
    }
}
