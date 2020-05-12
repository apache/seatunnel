/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config;

/**
 * A set of options related to resolving substitutions. Substitutions use the
 * <code>${foo.bar}</code> syntax and are documented in the <a
 * href="https://github.com/lightbend/config/blob/master/HOCON.md">HOCON</a>
 * spec.
 * <p>
 * Typically this class would be used with the method
 * {@link Config#resolve(ConfigResolveOptions)}.
 * <p>
 * This object is immutable, so the "setters" return a new object.
 * <p>
 * Here is an example of creating a custom {@code ConfigResolveOptions}:
 * 
 * <pre>
 *     ConfigResolveOptions options = ConfigResolveOptions.defaults()
 *         .setUseSystemEnvironment(false)
 * </pre>
 * <p>
 * In addition to {@link ConfigResolveOptions#defaults}, there's a prebuilt
 * {@link ConfigResolveOptions#noSystem} which avoids looking at any system
 * environment variables or other external system information. (Right now,
 * environment variables are the only example.)
 */
public final class ConfigResolveOptions {
    private final boolean useSystemEnvironment;
    private final boolean allowUnresolved;
    private final ConfigResolver resolver;

    private ConfigResolveOptions(boolean useSystemEnvironment, boolean allowUnresolved,
                                 ConfigResolver resolver) {
        this.useSystemEnvironment = useSystemEnvironment;
        this.allowUnresolved = allowUnresolved;
        this.resolver = resolver;
    }

    /**
     * Returns the default resolve options. By default the system environment
     * will be used and unresolved substitutions are not allowed.
     * 
     * @return the default resolve options
     */
    public static ConfigResolveOptions defaults() {
        return new ConfigResolveOptions(true, false, NULL_RESOLVER);
    }

    /**
     * Returns resolve options that disable any reference to "system" data
     * (currently, this means environment variables).
     *
     * @return the resolve options with env variables disabled
     */
    public static ConfigResolveOptions noSystem() {
        return defaults().setUseSystemEnvironment(false);
    }

    /**
     * Returns options with use of environment variables set to the given value.
     *
     * @param value
     *            true to resolve substitutions falling back to environment
     *            variables.
     * @return options with requested setting for use of environment variables
     */
    public ConfigResolveOptions setUseSystemEnvironment(boolean value) {
        return new ConfigResolveOptions(value, allowUnresolved, resolver);
    }

    /**
     * Returns whether the options enable use of system environment variables.
     * This method is mostly used by the config lib internally, not by
     * applications.
     *
     * @return true if environment variables should be used
     */
    public boolean getUseSystemEnvironment() {
        return useSystemEnvironment;
    }

    /**
     * Returns options with "allow unresolved" set to the given value. By
     * default, unresolved substitutions are an error. If unresolved
     * substitutions are allowed, then a future attempt to use the unresolved
     * value may fail, but {@link Config#resolve(ConfigResolveOptions)} itself
     * will not throw.
     * 
     * @param value
     *            true to silently ignore unresolved substitutions.
     * @return options with requested setting for whether to allow substitutions
     * @since 1.2.0
     */
    public ConfigResolveOptions setAllowUnresolved(boolean value) {
        return new ConfigResolveOptions(useSystemEnvironment, value, resolver);
    }

    /**
     * Returns options where the given resolver used as a fallback if a
     * reference cannot be otherwise resolved. This resolver will only be called
     * after resolution has failed to substitute with a value from within the
     * config itself and with any other resolvers that have been appended before
     * this one. Multiple resolvers can be added using,
     *
     *  <pre>
     *     ConfigResolveOptions options = ConfigResolveOptions.defaults()
     *         .appendResolver(primary)
     *         .appendResolver(secondary)
     *         .appendResolver(tertiary);
     * </pre>
     *
     * With this config unresolved references will first be resolved with the
     * primary resolver, if that fails then the secondary, and finally if that
     * also fails the tertiary.
     *
     * If all fallbacks fail to return a substitution "allow unresolved"
     * determines whether resolution fails or continues.
     *`
     * @param value the resolver to fall back to
     * @return options that use the given resolver as a fallback
     * @since 1.3.2
     */
    public ConfigResolveOptions appendResolver(ConfigResolver value) {
        if (value == null) {
            throw new ConfigException.BugOrBroken("null resolver passed to appendResolver");
        } else if (value == this.resolver) {
            return this;
        } else {
            return new ConfigResolveOptions(useSystemEnvironment, allowUnresolved,
                    this.resolver.withFallback(value));
        }
    }

    /**
     * Returns the resolver to use as a fallback if a substitution cannot be
     * otherwise resolved. Never returns null. This method is mostly used by the
     * config lib internally, not by applications.
     *
     * @return the non-null fallback resolver
     * @since 1.3.2
     */
    public ConfigResolver getResolver() {
        return this.resolver;
    }

    /**
     * Returns whether the options allow unresolved substitutions. This method
     * is mostly used by the config lib internally, not by applications.
     * 
     * @return true if unresolved substitutions are allowed
     * @since 1.2.0
     */
    public boolean getAllowUnresolved() {
        return allowUnresolved;
    }

    /**
     * Singleton resolver that never resolves paths.
     */
    private static final ConfigResolver NULL_RESOLVER = new ConfigResolver() {

        @Override
        public ConfigValue lookup(String path) {
            return null;
        }

        @Override
        public ConfigResolver withFallback(ConfigResolver fallback) {
            return fallback;
        }

    };

}
