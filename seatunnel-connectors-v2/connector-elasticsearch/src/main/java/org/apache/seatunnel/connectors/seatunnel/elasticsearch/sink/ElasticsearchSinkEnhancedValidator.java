package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.DefaultEnhancedConfigurationValidator;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog.ElasticSearchCatalogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.VECTORIZATION_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.VECTOR_DIMENSIONS;

public class ElasticsearchSinkEnhancedValidator extends DefaultEnhancedConfigurationValidator {

    public ElasticsearchSinkEnhancedValidator(String identifier) {
        super(identifier, PluginType.SINK);
    }

    @Override
    protected List<VersionCompatibilityRule> versionCompatibilityRules() {
        List<VersionCompatibilityRule> compatibilityRules = new ArrayList<>();
        Predicate<String> isEs73OrAbove =
                version -> {
                    if (version == null || version.isEmpty()) {
                        return false;
                    }
                    try {
                        String[] segments = version.split("\\.");
                        int major = Integer.parseInt(segments[0]);
                        int minor = segments.length > 1 ? Integer.parseInt(segments[1]) : 0;
                        return major > 7 || (major == 7 && minor >= 3);
                    } catch (NumberFormatException e) {
                        return false;
                    }
                };
        compatibilityRules.add(
                VersionCompatibilityRule.warning(VECTORIZATION_FIELDS, isEs73OrAbove, "7.3+"));
        compatibilityRules.add(
                VersionCompatibilityRule.warning(VECTOR_DIMENSIONS, isEs73OrAbove, "7.3+"));
        return compatibilityRules;
    }

    @Override
    protected Optional<Catalog> getCatalog(ReadonlyConfig context) {
        ElasticSearchCatalogFactory factory = new ElasticSearchCatalogFactory();
        return Optional.of(factory.createCatalog(identifier, context));
    }
}
