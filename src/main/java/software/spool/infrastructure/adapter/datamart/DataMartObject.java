package software.spool.infrastructure.adapter.datamart;

import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.infrastructure.adapter.dataLake.EnvelopeRecords;
import software.spool.infrastructure.adapter.dataLake.EnvelopeRecords.BinaryPayload;
import software.spool.infrastructure.adapter.dataLake.PayloadFormat;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A file a data mart keeps for a result of a mount, under {@code <silver|gold>/<dataMart>/<partition>/}.
 *
 * <p>A result is a single {@code <hash>.<extension>} with its content serialized. A result whose payload is binary is
 * kept as the data lake keeps an envelope: {@code <hash>.<extension>} with the payload as it came, the extension
 * given by its media type, and {@code <hash>.meta.json} with the rest of its fields. The name comes from the hash of
 * the result, so writing the same result again replaces its files instead of adding others.</p>
 *
 * @param path    where it goes, relative to the root of the data mart, with {@code /} between folders
 * @param content what the file holds
 */
public record DataMartObject(String path, byte[] content) {
    private static final String SEPARATOR = "/";
    private static final String EXTENSION_SEPARATOR = ".";

    /**
     * The files of a result, in the order they are written: a binary payload before the fields that name it.
     */
    public static List<DataMartObject> of(MountTarget target, PartitionedRecord<?> result, RecordSerializer<Object> serializer) {
        String folder = folderOf(target, result);
        if (!(result.record() instanceof GenericRecord generic)) {
            return List.of(whole(target, folder, serializer.serialize(result.record())));
        }
        Map<String, Object> fields = generic.toMap();
        return EnvelopeRecords.binaryPayloadOf(fields)
                .map(binary -> separated(target, folder, binary, serializer))
                .orElseGet(() -> List.of(whole(target, folder, serializer.serialize(fields))));
    }

    private static DataMartObject whole(MountTarget target, String folder, byte[] content) {
        String name = IdempotencyKey.of(target.dataMart(), content).value();
        return new DataMartObject(folder + SEPARATOR + name + EXTENSION_SEPARATOR + target.resolveExtension(content), content);
    }

    private static List<DataMartObject> separated(MountTarget target, String folder, BinaryPayload binary,
                                                  RecordSerializer<Object> serializer) {
        byte[] otherFields = serializer.serialize(binary.otherFields());
        String pathWithoutExtension = folder + SEPARATOR
                + IdempotencyKey.of(target.dataMart(), joined(otherFields, binary.bytes())).value();
        return List.of(
                new DataMartObject(pathWithoutExtension + EXTENSION_SEPARATOR + PayloadFormat.extensionOf(binary.mediaType()),
                        binary.bytes()),
                new DataMartObject(pathWithoutExtension + PayloadFormat.HEADER_SUFFIX, otherFields));
    }

    private static String folderOf(MountTarget target, PartitionedRecord<?> result) {
        return target.qualifiedDataMart() + KeyEntries.split(result.partitionKey().value()).stream()
                .map(entry -> SEPARATOR + entry)
                .collect(Collectors.joining());
    }

    private static byte[] joined(byte[] first, byte[] second) {
        return ByteBuffer.allocate(first.length + second.length).put(first).put(second).array();
    }
}
