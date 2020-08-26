package io.github.interestinglab.waterdrop.config;

/**
 * This method allows you to alter default config loading strategy for all the code which
 * calls {@link ConfigFactory#load}.
 *
 * Usually you don't have to implement this interface but it may be required
 * when you fixing a improperly implemented library with unavailable source code.
 *
 * You have to define VM property {@code config.strategy} to replace default strategy with your own.
 */
public interface ConfigLoadingStrategy {
    /**
     * This method must load and parse application config.
     *
     * @param parseOptions {@link ConfigParseOptions} to use
     * @return loaded config
     */
    Config parseApplicationConfig(ConfigParseOptions parseOptions);
}
