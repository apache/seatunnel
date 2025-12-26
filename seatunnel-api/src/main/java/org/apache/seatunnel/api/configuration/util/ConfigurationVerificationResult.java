package org.apache.seatunnel.api.configuration.util;

import lombok.Getter;

/**
 * Structured outcome for enhanced configuration validation.
 *
 * <p>Use this to report configuration issues without immediately throwing, so callers can decide
 * whether to warn or fail.
 */
@Getter
public class ConfigurationVerificationResult {

    public enum Level {
        ERROR,
        WARNING
    }

    private final Level level;
    private final String connector;
    private final String parameter;
    private final String message;
    private final String suggestion;

    public ConfigurationVerificationResult(
            Level level, String connector, String parameter, String message, String suggestion) {
        this.level = level;
        this.connector = connector;
        this.parameter = parameter;
        this.message = message;
        this.suggestion = suggestion;
    }

    public static ConfigurationVerificationResult error(
            String connector, String parameter, String message, String suggestion) {
        return new ConfigurationVerificationResult(
                Level.ERROR, connector, parameter, message, suggestion);
    }

    public static ConfigurationVerificationResult warning(
            String connector, String parameter, String message, String suggestion) {
        return new ConfigurationVerificationResult(
                Level.WARNING, connector, parameter, message, suggestion);
    }
}
