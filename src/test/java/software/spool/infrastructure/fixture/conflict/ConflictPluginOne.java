package software.spool.infrastructure.fixture.conflict;

import software.spool.infrastructure.fixture.StubPlugin;
import software.spool.infrastructure.spi.SpoolPlugin;

@SpoolPlugin(ConflictPort.class)
public class ConflictPluginOne extends StubPlugin<String> implements ConflictPort {
    public ConflictPluginOne() {
        super("CLASH", 10, true, "one");
    }
}
