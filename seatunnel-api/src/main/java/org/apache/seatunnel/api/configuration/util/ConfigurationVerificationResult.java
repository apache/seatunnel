package org.apache.seatunnel.api.configuration.util;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.common.constants.PluginType;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Structured outcome for enhanced configuration validation.
 *
 * <p>Use this to report configuration issues without immediately throwing, so callers can decide
 * whether to warn or fail.
 */
@Slf4j
@Getter
public class ConfigurationVerificationResult {

    private final Level level;
    private final String connectorIdentifier;
    private final PluginType pluginType;
    private final Option<?> option;
    private final String message;

    public ConfigurationVerificationResult(
            Level level,
            String connectorIdentifier,
            PluginType pluginType,
            Option<?> option,
            String message) {
        this.level = level;
        this.connectorIdentifier = connectorIdentifier;
        this.pluginType = pluginType;
        this.option = option;
        this.message = message;
    }

    public void log() {
        String optionKey = option == null ? "unknown" : option.key();
        switch (level) {
            case ERROR:
                log.error(
                        "[seatunnel][config-verification][{}][{}] option '{}' - {}",
                        pluginType.getType(),
                        connectorIdentifier,
                        optionKey,
                        message);
                break;
            case WARNING:
            default:
                log.warn(
                        "[seatunnel][config-verification][{}][{}] option '{}' - {}",
                        pluginType.getType(),
                        connectorIdentifier,
                        optionKey,
                        message);
                break;
        }
    }

    public static ConfigurationVerificationResult error(
            String connectorIdentifier, PluginType pluginType, Option<?> option, String message) {
        return new ConfigurationVerificationResult(
                Level.ERROR, connectorIdentifier, pluginType, option, message);
    }

    public static ConfigurationVerificationResult warning(
            String connectorIdentifier, PluginType pluginType, Option<?> option, String message) {
        return new ConfigurationVerificationResult(
                Level.WARNING, connectorIdentifier, pluginType, option, message);
    }

    public enum Level {
        ERROR,
        WARNING
    }
}
