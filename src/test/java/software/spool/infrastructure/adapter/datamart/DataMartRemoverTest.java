package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.model.vo.PartitionKey;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class DataMartRemoverTest {
    private static final String BUCKET = "spool-datamart-test";
    private static final MountTarget GOLD = MountTarget.aggregation("totals", new PartitionKey("source=payments"));
    private static final PartitionedRecord<?> SPAIN = new PartitionedRecord<>(new PartitionKey("country=ES"),
            GenericRecord.of(Map.of("country", "ES", "total", 10)));
    private static final PartitionedRecord<?> FRANCE = new PartitionedRecord<>(new PartitionKey("country=FR"),
            GenericRecord.of(Map.of("country", "FR", "total", 5)));
    private static final PartitionedRecord<?> REPORT = new PartitionedRecord<>(new PartitionKey("source=reports"),
            BinaryResults.pdfRecord());

    @TempDir
    Path folder;

    private final BoundedConcurrency concurrency = BoundedConcurrency.withThreads(2);
    private final InMemoryS3Client s3Client = new InMemoryS3Client();

    @AfterEach
    void closeConcurrency() {
        concurrency.close();
    }

    @Test
    void remove_fromTheFileSystem_removesOnlyThatResult() throws IOException {
        new FileSystemDataMartWriter(folder, concurrency).write(GOLD, Stream.of(SPAIN, FRANCE));

        new FileSystemDataMartRemover(folder.toString()).remove(GOLD, SPAIN);

        try (Stream<Path> files = Files.walk(folder)) {
            assertThat(files.filter(Files::isRegularFile).map(folder::relativize).map(Path::toString))
                    .singleElement().satisfies(file -> assertThat(file.replace('\\', '/')).startsWith("gold/totals/country=FR/"));
        }
    }

    @Test
    void remove_fromTheFileSystemAResultThatIsNotThere_doesNothing() {
        assertThatCode(() -> new FileSystemDataMartRemover(folder.toString()).remove(GOLD, SPAIN)).doesNotThrowAnyException();
    }

    @Test
    void remove_fromABucket_removesOnlyThatResult() {
        new S3DataMartWriter(s3Client, BUCKET, concurrency).write(GOLD, Stream.of(SPAIN, FRANCE));

        new S3DataMartRemover(s3Client, BUCKET).remove(GOLD, SPAIN);

        assertThat(s3Client.keys()).singleElement().satisfies(key -> assertThat(key).startsWith("gold/totals/country=FR/"));
    }

    @Test
    void remove_fromTheFileSystemAResultWithABinaryPayload_removesThePayloadAndTheRestOfTheRecord() throws IOException {
        new FileSystemDataMartWriter(folder, concurrency).write(GOLD, Stream.of(REPORT));

        new FileSystemDataMartRemover(folder.toString()).remove(GOLD, REPORT);

        try (Stream<Path> files = Files.walk(folder)) {
            assertThat(files.filter(Files::isRegularFile)).isEmpty();
        }
    }

    @Test
    void remove_fromABucketAResultWithABinaryPayload_removesThePayloadAndTheRestOfTheRecord() {
        new S3DataMartWriter(s3Client, BUCKET, concurrency).write(GOLD, Stream.of(REPORT));

        new S3DataMartRemover(s3Client, BUCKET).remove(GOLD, REPORT);

        assertThat(s3Client.keys()).isEmpty();
    }
}
