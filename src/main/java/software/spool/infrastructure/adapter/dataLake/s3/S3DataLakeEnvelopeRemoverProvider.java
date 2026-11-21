package software.spool.infrastructure.adapter.dataLake.s3;

import software.spool.core.port.datalake.DataLakeEnvelopeRemover;
import software.spool.infrastructure.adapter.s3.S3ClientFactory;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.DataLakeEnvelopeRemoverProvider;

@SpoolPlugin(DataLakeEnvelopeRemoverProvider.class)
public class S3DataLakeEnvelopeRemoverProvider implements DataLakeEnvelopeRemoverProvider {
    @Override
    public String name() {
        return "S3";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("region") &&
                configuration.has("bucket") &&
                configuration.has("endpoint");
    }

    @Override
    public DataLakeEnvelopeRemover create(PluginConfiguration configuration) {
        return new S3DataLakeEnvelopeRemover(S3ClientFactory.create(configuration), configuration.require("bucket"));
    }
}
