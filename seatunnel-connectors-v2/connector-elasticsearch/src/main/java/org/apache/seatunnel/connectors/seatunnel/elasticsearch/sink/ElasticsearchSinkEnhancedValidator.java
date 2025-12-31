package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.DefaultEnhancedConfigurationValidator;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog.ElasticSearchCatalog;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions;

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
        Predicate<String> isEs8OrAbove =
                version -> {
                    Integer majorVersion = parseMajorVersion(version);
                    return majorVersion != null && majorVersion >= 8;
                };
        compatibilityRules.add(
                VersionCompatibilityRule.warning(VECTORIZATION_FIELDS, isEs8OrAbove, "8"));
        compatibilityRules.add(
                VersionCompatibilityRule.warning(VECTOR_DIMENSIONS, isEs8OrAbove, "8"));
        return compatibilityRules;
    }

    @Override
    protected Optional<Catalog> getCatalog(ReadonlyConfig context) {
        String defaultDatabase = context.getOptional(ElasticsearchBaseOptions.INDEX).orElse("");
        return Optional.of(new ElasticSearchCatalog(identifier, defaultDatabase, context));
    }

    private Integer parseMajorVersion(String version) {
        if (version == null || version.isEmpty()) {
            return null;
        }
        try {
            String[] segments = version.split("\\.");
            return Integer.parseInt(segments[0]);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
