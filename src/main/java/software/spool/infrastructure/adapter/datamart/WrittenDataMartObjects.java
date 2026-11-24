package software.spool.infrastructure.adapter.datamart;

import software.spool.mounter.api.port.MountTarget;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The objects a data mart writer has written for each target since it was last asked, grouped by the folder of their
 * partition, so the writer can remove from those folders what it did not write.
 */
final class WrittenDataMartObjects {
    private static final String SEPARATOR = "/";

    private final Map<MountTarget, Map<String, Set<String>>> written = new ConcurrentHashMap<>();

    void note(MountTarget target, DataMartObject object) {
        written.computeIfAbsent(target, ignored -> new ConcurrentHashMap<>())
                .computeIfAbsent(folderOf(object.path()), ignored -> ConcurrentHashMap.newKeySet())
                .add(object.path());
    }

    /** The paths written for the target by folder, which it forgets. */
    Map<String, Set<String>> take(MountTarget target) {
        Map<String, Set<String>> taken = written.remove(target);
        return taken == null ? Map.of() : taken;
    }

    static String folderOf(String path) {
        return path.substring(0, path.lastIndexOf(SEPARATOR));
    }

    static String pathIn(String folder, String name) {
        return folder + SEPARATOR + name;
    }
}
