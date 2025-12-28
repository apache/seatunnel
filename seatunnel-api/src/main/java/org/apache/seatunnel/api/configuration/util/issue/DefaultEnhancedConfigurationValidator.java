package org.apache.seatunnel.api.configuration.util.issue;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.EnhancedConfigurationValidator;

import java.util.Collections;
import java.util.List;

public abstract class DefaultEnhancedConfigurationValidator
        implements EnhancedConfigurationValidator {

    @Override
    public List<ConfigurationVerificationIssue> validateDeprecatedRules(ReadonlyConfig context) {
        return Collections.emptyList();
    }

    @Override
    public List<ConfigurationVerificationIssue> validateConflictRules(ReadonlyConfig context) {
        return Collections.emptyList();
    }

    @Override
    public List<ConfigurationVerificationIssue> validateVersionCompatibilityRules(
            ReadonlyConfig context) {
        return Collections.emptyList();
    }
}
