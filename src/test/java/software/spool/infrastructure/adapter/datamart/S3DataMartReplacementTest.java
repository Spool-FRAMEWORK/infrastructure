package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.AfterEach;
import software.spool.infrastructure.adapter.s3.InMemoryS3Client;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.utils.BoundedConcurrency;

import java.util.List;

class S3DataMartReplacementTest extends DataMartReplacementContract {
    private static final String BUCKET = "spool-datamart-test";

    private final InMemoryS3Client s3Client = new InMemoryS3Client();
    private final BoundedConcurrency concurrency = BoundedConcurrency.withThreads(2);
    private final S3DataMartWriter writer = new S3DataMartWriter(s3Client, BUCKET, concurrency);

    @AfterEach
    void closeConcurrency() {
        concurrency.close();
    }

    @Override
    DataMartWriter writer() {
        return writer;
    }

    @Override
    List<String> objects() {
        return s3Client.keys().stream().sorted().toList();
    }
}
