package software.spool.infrastructure.adapter.dataLake.postgresql;

import software.spool.infrastructure.provisioning.PieceProvisioning;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.InvalidPluginConfigurationException;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.provisioning.ProvisioningProvider;

import java.util.regex.Pattern;

/**
 * Generates the Terraform of the PostgreSQL stores of envelope headers. The password has to come from the environment,
 * as {@code ${env:NAME}}, which the generator turns into a Terraform variable: written in the descriptor, it would end
 * up in the generated files.
 */
@SpoolPlugin(ProvisioningProvider.class)
public class PostgreSqlProvisioningProvider implements ProvisioningProvider {
    private static final Pattern TERRAFORM_VARIABLE = Pattern.compile("\\$\\{var\\.[A-Za-z_][A-Za-z0-9_-]*}");
    private static final String DEFAULT_USER = "postgres";

    @Override
    public String name() {
        return PostgreSqlEnvelopeHeaderStores.TYPE;
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has(PostgreSqlEnvelopeHeaderStores.URL);
    }

    @Override
    public PieceProvisioning create(PluginConfiguration configuration) {
        return new PostgreSqlProvisioning(configuration.require(PIECE, String.class),
                PostgreSqlAddress.of(configuration.require(PostgreSqlEnvelopeHeaderStores.URL)),
                configuration.get(PostgreSqlEnvelopeHeaderStores.USER).orElse(DEFAULT_USER),
                passwordOf(configuration));
    }

    private static String passwordOf(PluginConfiguration configuration) {
        String password = configuration.get(PostgreSqlEnvelopeHeaderStores.PASSWORD)
                .orElseThrow(() -> unsafe("is required to generate the Terraform of the database"));
        if (!TERRAFORM_VARIABLE.matcher(password).matches())
            throw unsafe("must not be written in the descriptor");
        return password;
    }

    private static InvalidPluginConfigurationException unsafe(String problem) {
        return new InvalidPluginConfigurationException(PostgreSqlEnvelopeHeaderStores.PASSWORD,
                problem + "; give it from the environment, as ${env:NAME}");
    }
}
