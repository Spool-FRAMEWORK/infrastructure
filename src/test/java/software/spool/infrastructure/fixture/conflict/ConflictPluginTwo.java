package software.spool.infrastructure.fixture.conflict;

import software.spool.infrastructure.fixture.StubPlugin;
import software.spool.infrastructure.spi.SpoolPlugin;

@SpoolPlugin(ConflictPort.class)
public class ConflictPluginTwo extends StubPlugin<String> implements ConflictPort {
    public ConflictPluginTwo() {
        super("CLASH", 10, true, "two");
    }
}
