package software.spool.infrastructure.adapter.aggregate;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.AggregateMetadata;
import software.spool.mounter.api.model.ConsumedPartition;
import software.spool.mounter.api.port.AggregateMetadataRepository;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Keeps the metadata of the aggregates in JSON files laid out by {@link AggregateMetadataLayout}. Each file is written
 * next to its place and then moved there, so a reader never sees half a file.
 */
public class FileSystemAggregateMetadataRepository implements AggregateMetadataRepository {
    private static final String PARTIAL_SUFFIX = ".partial";

    private final Path basePath;

    public FileSystemAggregateMetadataRepository(Path basePath) {
        this.basePath = basePath;
    }

    public FileSystemAggregateMetadataRepository(String basePath) {
        this(Path.of(basePath));
    }

    @Override
    public Optional<AggregateMetadata> find(String dataMart, PartitionKey aggregate) {
        Path file = basePath.resolve(AggregateMetadataLayout.aggregateKeyOf(dataMart, aggregate));
        return Files.exists(file) ? Optional.of(AggregateMetadataDocument.deserialize(read(file))) : Optional.empty();
    }

    @Override
    public List<AggregateMetadata> findAll(String dataMart) {
        Path folder = basePath.resolve(AggregateMetadataLayout.aggregatesFolderOf(dataMart));
        if (!Files.isDirectory(folder)) return List.of();
        try (Stream<Path> files = Files.list(folder)) {
            return files.filter(file -> AggregateMetadataLayout.isAggregateKey(file.getFileName().toString()))
                    .sorted()
                    .map(file -> AggregateMetadataDocument.deserialize(read(file)))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Aggregate metadata of " + dataMart + " could not be listed in " + folder, e);
        }
    }

    @Override
    public void save(AggregateMetadata metadata) {
        write(basePath.resolve(AggregateMetadataLayout.aggregateKeyOf(metadata.dataMart(), metadata.aggregate())),
                AggregateMetadataDocument.serialize(metadata));
    }

    @Override
    public Set<ConsumedPartition> consumedSources(String dataMart) {
        Path file = basePath.resolve(AggregateMetadataLayout.consumedSourcesKeyOf(dataMart));
        return Files.exists(file) ? ConsumedPartitionsDocument.deserialize(read(file)) : Set.of();
    }

    @Override
    public void markConsumed(String dataMart, Set<ConsumedPartition> sources) {
        if (sources.isEmpty()) return;
        write(basePath.resolve(AggregateMetadataLayout.consumedSourcesKeyOf(dataMart)),
                ConsumedPartitionsDocument.serialize(ConsumedPartitionsDocument.merged(consumedSources(dataMart), sources)));
    }

    private static byte[] read(Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException("Aggregate metadata could not be read from " + file, e);
        }
    }

    private static void write(Path file, byte[] content) {
        Path partial = file.resolveSibling(file.getFileName() + PARTIAL_SUFFIX);
        try {
            Files.createDirectories(file.getParent());
            Files.write(partial, content);
            Files.move(partial, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("Aggregate metadata could not be written to " + file, e);
        }
    }
}
