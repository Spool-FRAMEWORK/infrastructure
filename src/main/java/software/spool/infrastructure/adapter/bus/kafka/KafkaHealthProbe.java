package software.spool.infrastructure.adapter.bus.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import software.spool.core.port.health.HealthCheck;
import software.spool.core.port.health.HealthProbe;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Checks that the brokers of a Kafka piece answer, asking for the id of their cluster within a time limit.
 */
public final class KafkaHealthProbe implements HealthProbe {
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);
    private static final String NAME = "Kafka";

    private final String bootstrapServers;
    private final Duration timeout;

    public KafkaHealthProbe(String bootstrapServers, Duration timeout) {
        this.bootstrapServers = bootstrapServers;
        this.timeout = timeout;
    }

    @Override
    public HealthCheck probe() {
        int timeoutMillis = Math.toIntExact(timeout.toMillis());
        Map<String, Object> configuration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMillis,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMillis);
        try (Admin admin = Admin.create(configuration)) {
            admin.describeCluster(new DescribeClusterOptions().timeoutMs(timeoutMillis)).clusterId()
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return HealthCheck.healthy(NAME);
        } catch (ExecutionException e) {
            return unreachable(e.getCause().getMessage());
        } catch (TimeoutException e) {
            return unreachable("no answer within " + timeout.toMillis() + " ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return unreachable("the check was interrupted");
        } catch (RuntimeException e) {
            return unreachable(e.getMessage());
        }
    }

    private HealthCheck unreachable(String reason) {
        return HealthCheck.unhealthy(NAME, "The brokers " + bootstrapServers + " do not answer: " + reason);
    }
}
