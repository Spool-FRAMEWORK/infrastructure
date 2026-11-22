package software.spool.infrastructure.adapter.datamart;

import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.stream.Collectors;

/**
 * A result of a mount as a data mart keeps it: its content, serialized, at
 * {@code <silver|gold>/<dataMart>/<partition>/<hash>.<extension>}. The name comes from the hash of the content, so
 * writing the same result again replaces it instead of adding another.
 *
 * @param path    where it goes, relative to the root of the data mart, with {@code /} between folders
 * @param content its serialized content
 */
public record DataMartObject(String path, byte[] content) {
    private static final String SEPARATOR = "/";
    private static final String EXTENSION_SEPARATOR = ".";

    public static DataMartObject of(MountTarget target, PartitionedRecord<?> result, RecordSerializer<Object> serializer) {
        byte[] content = serializer.serialize(contentOf(result.record()));
        String folder = target.qualifiedDataMart() + KeyEntries.split(result.partitionKey().value()).stream()
                .map(entry -> SEPARATOR + entry)
                .collect(Collectors.joining());
        String name = IdempotencyKey.of(target.dataMart(), content).value() + EXTENSION_SEPARATOR + target.resolveExtension(content);
        return new DataMartObject(folder + SEPARATOR + name, content);
    }

    private static Object contentOf(Object record) {
        return record instanceof GenericRecord generic ? generic.toMap() : record;
    }
}
