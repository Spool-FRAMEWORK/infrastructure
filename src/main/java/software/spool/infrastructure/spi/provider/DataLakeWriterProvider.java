package software.spool.infrastructure.spi.provider;

import software.spool.infrastructure.spi.Plugin;
import software.spool.ingester.api.port.DataLakeWriter;

public interface DataLakeWriterProvider extends Plugin<DataLakeWriter> {
}
