package org.apache.seatunnel.api.configuration.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * An enhanced configuration validator used to perform rule-based verification on connector or
 * component configurations.
 *
 * <p>This validator focuses on non-fatal configuration issues such as deprecations, conflicts, and
 * version compatibility. Validation results are returned as structured verification outputs rather
 * than throwing exceptions directly, allowing the caller to aggregate and classify errors and
 * warnings.
 */
public interface EnhancedConfigurationValidator {

    /**
     * Execute all configuration validation rules and aggregate the results.
     *
     * <p>This method serves as a unified entry point for configuration verification. It
     * sequentially executes all supported validation categories, including:
     *
     * <ul>
     *   <li>Deprecated parameter validation
     *   <li>Conflicting parameter validation
     *   <li>Version compatibility validation
     * </ul>
     *
     * <p>The method aggregates verification results from each rule category into a single list,
     * allowing the caller to process all validation outcomes in a centralized manner (e.g.,
     * grouping by severity, logging warnings, or throwing exceptions for errors).
     *
     * <p>Implementations are encouraged to return an empty list instead of {@code null}. This
     * default implementation defensively handles {@code null} returns to ensure robustness.
     *
     * @return a combined list of configuration verification results from all validation rules; an
     *     empty list if no issues are detected
     */
    default List<ConfigurationVerificationResult> validate() {
        List<ConfigurationVerificationResult> results = new ArrayList<>();
        Optional.ofNullable(validateDeprecatedRules()).ifPresent(results::addAll);
        Optional.ofNullable(validateConflictRules()).ifPresent(results::addAll);
        Optional.ofNullable(validateVersionCompatibilityRules()).ifPresent(results::addAll);
        return results;
    }

    /**
     * Validate rules related to deprecated configuration parameters.
     *
     * <p>This validation checks whether any configuration parameters have been marked as deprecated
     * and provides guidance or suggestions for recommended replacements.
     *
     * <p>Typically, deprecated parameters should produce warning-level verification results rather
     * than fatal errors.
     *
     * @return a list of verification results describing deprecated parameters and suggested
     *     alternatives; empty if none found
     */
    List<ConfigurationVerificationResult> validateDeprecatedRules();

    /**
     * Validate rules related to conflicting configuration parameters.
     *
     * <p>This validation detects mutually exclusive, logically conflicting, or incompatible
     * configuration options. Conflicts usually indicate misconfiguration and should be treated as
     * error-level results.
     *
     * <p>Examples include parameters that cannot be enabled at the same time or options whose
     * semantics override each other.
     *
     * @return a list of verification results describing configuration conflicts; empty if no
     *     conflicts are detected
     */
    List<ConfigurationVerificationResult> validateConflictRules();

    /**
     * Validate rules related to version compatibility and constraints.
     *
     * <p>This validation ensures that configuration parameters are compatible with the runtime
     * environment, connector version, or external system version.
     *
     * <p>Examples include parameters introduced in specific versions, options removed in later
     * versions, or features only supported by certain engine or service versions.
     *
     * @return a list of verification results describing version incompatibilities or constraints;
     *     empty if all parameters are version-compatible
     */
    List<ConfigurationVerificationResult> validateVersionCompatibilityRules();
}
