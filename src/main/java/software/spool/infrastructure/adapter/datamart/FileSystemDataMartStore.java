package software.spool.infrastructure.adapter.datamart;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/** The objects of a data mart as files under a folder of the file system. */
final class FileSystemDataMartStore implements DataMartStore {
    private static final String SEPARATOR = "/";

    private final Path basePath;

    FileSystemDataMartStore(Path basePath) {
        this.basePath = basePath;
    }

    @Override
    public List<String> objectsIn(String folder) {
        Path directory = basePath.resolve(folder);
        if (!Files.isDirectory(directory)) return List.of();
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(Files::isRegularFile).map(this::pathOf).toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Data mart folder could not be listed: " + folder, e);
        }
    }

    @Override
    public List<String> objectsUnder(String prefix) {
        Path directory = basePath.resolve(prefix);
        if (!Files.isDirectory(directory)) return List.of();
        try (Stream<Path> files = Files.walk(directory)) {
            return files.filter(Files::isRegularFile).map(this::pathOf).toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Data mart folder could not be listed: " + prefix, e);
        }
    }

    @Override
    public Optional<byte[]> read(String path) {
        try {
            return Optional.of(Files.readAllBytes(basePath.resolve(path)));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        } catch (IOException e) {
            throw new UncheckedIOException("Data mart object could not be read: " + path, e);
        }
    }

    @Override
    public void write(String path, byte[] content) {
        Path file = basePath.resolve(path);
        try {
            Files.createDirectories(file.getParent());
            Files.write(file, content);
        } catch (IOException e) {
            throw new UncheckedIOException("Data mart object could not be written: " + path, e);
        }
    }

    @Override
    public boolean createIfAbsent(String path, byte[] content) {
        Path file = basePath.resolve(path);
        try {
            Files.createDirectories(file.getParent());
            Files.write(file, content, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            return true;
        } catch (FileAlreadyExistsException e) {
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException("Data mart object could not be created: " + path, e);
        }
    }

    @Override
    public void delete(String path) {
        try {
            Files.deleteIfExists(basePath.resolve(path));
        } catch (IOException e) {
            throw new UncheckedIOException("Data mart object could not be removed: " + path, e);
        }
    }

    private String pathOf(Path file) {
        return basePath.relativize(file).toString().replace(File.separator, SEPARATOR);
    }
}
