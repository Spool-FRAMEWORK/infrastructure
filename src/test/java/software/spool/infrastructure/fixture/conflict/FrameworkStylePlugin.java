package software.spool.infrastructure.fixture.conflict;

import software.spool.infrastructure.fixture.StubPlugin;
import software.spool.infrastructure.spi.SpoolPlugin;

@SpoolPlugin(OverridePort.class)
public class FrameworkStylePlugin extends StubPlugin<String> implements OverridePort {
    public FrameworkStylePlugin() {
        super("S3", 10, true, "framework");
    }
}
