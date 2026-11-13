package software.spool.infrastructure.adapter.inbox.memory;

import software.spool.core.adapter.memory.InMemoryInbox;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.infrastructure.adapter.inbox.InboxUpdaterContract;

import java.util.Optional;

class InMemoryInboxUpdaterContractTest extends InboxUpdaterContract {

    private final InMemoryInbox inbox = new InMemoryInbox();

    @Override
    protected InboxUpdater updater() {
        return inbox;
    }

    @Override
    protected void store(Envelope envelope) {
        inbox.update(envelope);
    }

    @Override
    protected Optional<Envelope> find(IdempotencyKey idempotencyKey) {
        return inbox.findById(idempotencyKey);
    }
}
