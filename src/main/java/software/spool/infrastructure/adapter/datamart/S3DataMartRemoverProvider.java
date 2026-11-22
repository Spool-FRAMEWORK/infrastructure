package software.spool.infrastructure.adapter.datamart;

import software.spool.infrastructure.adapter.s3.S3ClientFactory;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.datamart.DataMartRemoverProvider;
import software.spool.mounter.api.port.DataMartRemover;

@SpoolPlugin(DataMartRemoverProvider.class)
public class S3DataMartRemoverProvider implements DataMartRemoverProvider {
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
    public DataMartRemover create(PluginConfiguration configuration) {
        return new S3DataMartRemover(S3ClientFactory.create(configuration), configuration.require("bucket"));
    }
}
