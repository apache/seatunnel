package org.apache.seatunnel.api.configuration.util.issue;

import lombok.AllArgsConstructor;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.EnhancedConfigurationValidator;
import org.apache.seatunnel.common.constants.PluginType;

import java.util.Collections;
import java.util.List;

@AllArgsConstructor
public abstract class DefaultEnhancedConfigurationValidator
        implements EnhancedConfigurationValidator {

    protected final String identifier;
    protected final PluginType pluginType;
    //protected final

    @Override
    public List<ConfigurationVerificationIssue> validateDeprecatedRules(ReadonlyConfig context) {
        final List<Option<?>> deprecateOptions = deprecatedOptions(context);
        if (deprecateOptions.isEmpty()) {
            return Collections.emptyList();
        }
        deprecateOptions.forEach(option -> {

        });
    }

    protected abstract List<Option<?>> deprecatedOptions(ReadonlyConfig context);

    @Override
    public List<ConfigurationVerificationIssue> validateConflictRules(ReadonlyConfig context) {

    }

    @Override
    public List<ConfigurationVerificationIssue> validateVersionCompatibilityRules(
            ReadonlyConfig context) {

    }
}
