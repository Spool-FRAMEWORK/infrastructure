package software.spool.infrastructure.spi.provider.datamart;

import software.spool.infrastructure.spi.Plugin;
import software.spool.mounter.api.port.DataMartWriter;

public interface DataMartWriterProvider extends Plugin<DataMartWriter<?>> {
}
