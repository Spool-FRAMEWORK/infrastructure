package software.spool.infrastructure.provisioning;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The part of the Terraform a piece brings, in the shape of Terraform's JSON syntax: resources by type and name, the
 * providers it needs and how they are configured, the variables it reads and, in AWS, the access a module that uses
 * the piece needs.
 */
public record TerraformFragment(
        Map<String, Map<String, Map<String, Object>>> resources,
        Map<String, Map<String, Object>> requiredProviders,
        Map<String, Map<String, Object>> providers,
        Map<String, Map<String, Object>> variables,
        List<ResourceAccess> accesses
) {
    public TerraformFragment {
        resources = copyOf(resources);
        requiredProviders = Map.copyOf(requiredProviders);
        providers = Map.copyOf(providers);
        variables = Map.copyOf(variables);
        accesses = List.copyOf(accesses);
    }

    public static TerraformFragment empty() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Map<String, Map<String, Map<String, Object>>> copyOf(Map<String, Map<String, Map<String, Object>>> resources) {
        Map<String, Map<String, Map<String, Object>>> copy = new LinkedHashMap<>();
        resources.forEach((type, byName) -> copy.put(type, Map.copyOf(byName)));
        return Map.copyOf(copy);
    }

    public static final class Builder {
        private final Map<String, Map<String, Map<String, Object>>> resources = new LinkedHashMap<>();
        private final Map<String, Map<String, Object>> requiredProviders = new LinkedHashMap<>();
        private final Map<String, Map<String, Object>> providers = new LinkedHashMap<>();
        private final Map<String, Map<String, Object>> variables = new LinkedHashMap<>();
        private final List<ResourceAccess> accesses = new ArrayList<>();

        private Builder() {}

        public Builder resource(String type, String name, Map<String, Object> body) {
            resources.computeIfAbsent(type, ignored -> new LinkedHashMap<>()).put(name, body);
            return this;
        }

        /** A provider the resources need, by its local name, with its source such as {@code hashicorp/aws}. */
        public Builder requiredProvider(String name, String source, String version) {
            requiredProviders.put(name, Map.of("source", source, "version", version));
            return this;
        }

        public Builder provider(String name, Map<String, Object> configuration) {
            providers.put(name, configuration);
            return this;
        }

        public Builder variable(String name, Map<String, Object> declaration) {
            variables.put(name, declaration);
            return this;
        }

        public Builder access(ResourceAccess access) {
            accesses.add(access);
            return this;
        }

        public TerraformFragment build() {
            return new TerraformFragment(resources, requiredProviders, providers, variables, accesses);
        }
    }
}
