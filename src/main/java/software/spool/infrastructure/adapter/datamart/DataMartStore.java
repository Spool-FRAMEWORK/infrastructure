package software.spool.infrastructure.adapter.datamart;

import java.util.List;
import java.util.Optional;

/**
 * The objects a data mart keeps, by their path from its root with {@code /} between folders, as a file system or a
 * bucket holds them.
 */
interface DataMartStore {
    /** The paths of the objects directly inside the folder, none if it does not exist. */
    List<String> objectsIn(String folder);

    /** The paths of every object under the prefix, at any depth. */
    List<String> objectsUnder(String prefix);

    Optional<byte[]> read(String path);

    void write(String path, byte[] content);

    /**
     * Creates the object only if there is none at the path, atomically, so of several callers at once only one creates
     * it.
     *
     * @return whether this call created it
     */
    boolean createIfAbsent(String path, byte[] content);

    /** Removes the object, if it is there. */
    void delete(String path);
}
