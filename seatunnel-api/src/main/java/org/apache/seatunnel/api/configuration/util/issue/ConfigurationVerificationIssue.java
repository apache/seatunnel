package org.apache.seatunnel.api.configuration.util.issue;

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
public abstract class ConfigurationVerificationIssue {

    private final Level level;
    private final String identifier;
    private final PluginType pluginType;
    private final Option<?> option;

    public ConfigurationVerificationIssue(
            Level level, String identifier, PluginType pluginType, Option<?> option) {
        this.level = level;
        this.identifier = identifier;
        this.pluginType = pluginType;
        this.option = option;
    }

    public abstract void log();

    public enum Level {
        ERROR,
        WARNING
    }
}
