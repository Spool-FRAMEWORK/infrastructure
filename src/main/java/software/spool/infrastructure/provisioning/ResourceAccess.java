package software.spool.infrastructure.provisioning;

import java.util.List;

/** What a module that uses a piece may do with it in AWS: the actions it may take on those resources. */
public record ResourceAccess(List<String> resources, List<String> actions) {
    public ResourceAccess {
        resources = List.copyOf(resources);
        actions = List.copyOf(actions);
    }
}
