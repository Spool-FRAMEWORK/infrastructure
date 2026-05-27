package software.spool.infrastructure.adapter.dataLake.filesystem;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.PartitionedReaderProvider;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.PartitionedReader;

import java.util.Base64;
import java.util.Map;

@SpoolPlugin(PartitionedReaderProvider.class)
public class FileSystemPartitionedReaderProvider implements PartitionedReaderProvider {
    @Override
    public String name() {
        return "FILE_SYSTEM";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("path");
    }

    @Override
    public PartitionedReader<GenericRecord> create(PluginConfiguration configuration) {
        return new FileSystemPartitionedReader<>(
                configuration.require("path"),
                buildDeserializer()
        );
    }

    private static PayloadDeserializer<GenericRecord> buildDeserializer() {
        return bytes -> {
            Map<String, Object> envelope = PayloadDeserializerFactory.json().asMap().deserialize(bytes);
            String encoded = (String) envelope.get("payload");
            byte[] decoded = Base64.getDecoder().decode(encoded);
            envelope.put("payload", PayloadDeserializerFactory.json().asMap().deserialize(decoded));
            return GenericRecord.of(envelope);
        };
    }
}
