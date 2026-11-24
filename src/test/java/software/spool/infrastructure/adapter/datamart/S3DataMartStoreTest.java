package software.spool.infrastructure.adapter.datamart;

import software.spool.infrastructure.adapter.s3.InMemoryS3Client;

class S3DataMartStoreTest extends DataMartStoreContract {
    private final DataMartStore store = new S3DataMartStore(new InMemoryS3Client(), "spool-datamart-test");

    @Override
    DataMartStore store() {
        return store;
    }
}
