package software.spool.infrastructure.fixture.conflict;

import software.spool.infrastructure.fixture.StubPlugin;
import software.spool.infrastructure.spi.SpoolPlugin;

@SpoolPlugin(OverridePort.class)
public class CustomStylePlugin extends StubPlugin<String> implements OverridePort {
    public CustomStylePlugin() {
        super("S3", 5, true, "custom");
    }
}
