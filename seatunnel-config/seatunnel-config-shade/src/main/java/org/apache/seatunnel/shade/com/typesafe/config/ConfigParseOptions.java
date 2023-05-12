/*
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */

package org.apache.seatunnel.shade.com.typesafe.config;

/**
 * A set of options related to parsing.
 *
 * <p>This object is immutable, so the "setters" return a new object.
 *
 * <p>Here is an example of creating a custom {@code ConfigParseOptions}:
 *
 * <pre>
 *     ConfigParseOptions options = ConfigParseOptions.defaults()
 *         .setSyntax(ConfigSyntax.JSON)
 *         .setAllowMissing(false)
 * </pre>
 */
public final class ConfigParseOptions {

    /** a.b.c a-&gt;b-&gt;c */
    public static final String PATH_TOKEN_SEPARATOR = "->";

    final ConfigSyntax syntax;
    final String originDescription;
    final boolean allowMissing;
    final ConfigIncluder includer;
    final ClassLoader classLoader;

    private ConfigParseOptions(
            ConfigSyntax syntax,
            String originDescription,
            boolean allowMissing,
            ConfigIncluder includer,
            ClassLoader classLoader) {
        this.syntax = syntax;
        this.originDescription = originDescription;
        this.allowMissing = allowMissing;
        this.includer = includer;
        this.classLoader = classLoader;
    }

    /**
     * Gets an instance of {@code ConfigParseOptions} with all fields set to the default values.
     * Start with this instance and make any changes you need.
     *
     * @return the default parse options
     */
    public static ConfigParseOptions defaults() {
        return new ConfigParseOptions(null, null, true, null, null);
    }

    /**
     * Set the file format. If set to null, try to guess from any available filename extension; if
     * guessing fails, assume {@link ConfigSyntax#CONF}.
     *
     * @param syntax a syntax or {@code null} for best guess
     * @return options with the syntax set
     */
    public ConfigParseOptions setSyntax(ConfigSyntax syntax) {
        if (this.syntax == syntax) {
            return this;
        } else {
            return new ConfigParseOptions(
                    syntax,
                    this.originDescription,
                    this.allowMissing,
                    this.includer,
                    this.classLoader);
        }
    }

    /**
     * Gets the current syntax option, which may be null for "any".
     *
     * @return the current syntax or null
     */
    public ConfigSyntax getSyntax() {
        return syntax;
    }

    /**
     * Set a description for the thing being parsed. In most cases this will be set up for you to
     * something like the filename, but if you provide just an input stream you might want to
     * improve on it. Set to null to allow the library to come up with something automatically. This
     * description is the basis for the {@link ConfigOrigin} of the parsed values.
     *
     * @param originDescription description to put in the {@link ConfigOrigin}
     * @return options with the origin description set
     */
    public ConfigParseOptions setOriginDescription(String originDescription) {
        // findbugs complains about == here but is wrong, do not "fix"
        if (this.originDescription == originDescription) {
            return this;
        } else if (this.originDescription != null
                && originDescription != null
                && this.originDescription.equals(originDescription)) {
            return this;
        } else {
            return new ConfigParseOptions(
                    this.syntax,
                    originDescription,
                    this.allowMissing,
                    this.includer,
                    this.classLoader);
        }
    }

    /**
     * Gets the current origin description, which may be null for "automatic".
     *
     * @return the current origin description or null
     */
    public String getOriginDescription() {
        return originDescription;
    }

    /** this is package-private, not public API */
    ConfigParseOptions withFallbackOriginDescription(String originDescription) {
        if (this.originDescription == null) {
            return setOriginDescription(originDescription);
        } else {
            return this;
        }
    }

    /**
     * Set to false to throw an exception if the item being parsed (for example a file) is missing.
     * Set to true to just return an empty document in that case. Note that this setting applies on
     * only to fetching the root document, it has no effect on any nested includes.
     *
     * @param allowMissing true to silently ignore missing item
     * @return options with the "allow missing" flag set
     */
    public ConfigParseOptions setAllowMissing(boolean allowMissing) {
        if (this.allowMissing == allowMissing) {
            return this;
        } else {
            return new ConfigParseOptions(
                    this.syntax,
                    this.originDescription,
                    allowMissing,
                    this.includer,
                    this.classLoader);
        }
    }

    /**
     * Gets the current "allow missing" flag.
     *
     * @return whether we allow missing files
     */
    public boolean getAllowMissing() {
        return allowMissing;
    }

    /**
     * Set a {@link ConfigIncluder} which customizes how includes are handled. null means to use the
     * default includer.
     *
     * @param includer the includer to use or null for default
     * @return new version of the parse options with different includer
     */
    public ConfigParseOptions setIncluder(ConfigIncluder includer) {
        if (this.includer == includer) {
            return this;
        } else {
            return new ConfigParseOptions(
                    this.syntax,
                    this.originDescription,
                    this.allowMissing,
                    includer,
                    this.classLoader);
        }
    }

    /**
     * Prepends a {@link ConfigIncluder} which customizes how includes are handled. To prepend your
     * includer, the library calls {@link ConfigIncluder#withFallback} on your includer to append
     * the existing includer to it.
     *
     * @param includer the includer to prepend (may not be null)
     * @return new version of the parse options with different includer
     */
    public ConfigParseOptions prependIncluder(ConfigIncluder includer) {
        if (includer == null) {
            throw new NullPointerException("null includer passed to prependIncluder");
        }
        if (this.includer == includer) {
            return this;
        } else if (this.includer != null) {
            return setIncluder(includer.withFallback(this.includer));
        } else {
            return setIncluder(includer);
        }
    }

    /**
     * Appends a {@link ConfigIncluder} which customizes how includes are handled. To append, the
     * library calls {@link ConfigIncluder#withFallback} on the existing includer.
     *
     * @param includer the includer to append (may not be null)
     * @return new version of the parse options with different includer
     */
    public ConfigParseOptions appendIncluder(ConfigIncluder includer) {
        if (includer == null) {
            throw new NullPointerException("null includer passed to appendIncluder");
        }
        if (this.includer == includer) {
            return this;
        } else if (this.includer != null) {
            return setIncluder(this.includer.withFallback(includer));
        } else {
            return setIncluder(includer);
        }
    }

    /**
     * Gets the current includer (will be null for the default includer).
     *
     * @return current includer or null
     */
    public ConfigIncluder getIncluder() {
        return includer;
    }

    /**
     * Set the class loader. If set to null, {@code Thread.currentThread().getContextClassLoader()}
     * will be used.
     *
     * @param loader a class loader or {@code null} to use thread context class loader
     * @return options with the class loader set
     */
    public ConfigParseOptions setClassLoader(ClassLoader loader) {
        if (this.classLoader == loader) {
            return this;
        } else {
            return new ConfigParseOptions(
                    this.syntax, this.originDescription, this.allowMissing, this.includer, loader);
        }
    }

    /**
     * Get the class loader; never returns {@code null}, if the class loader was unset, returns
     * {@code Thread.currentThread().getContextClassLoader()}.
     *
     * @return class loader to use
     */
    public ClassLoader getClassLoader() {
        if (this.classLoader == null) {
            return Thread.currentThread().getContextClassLoader();
        } else {
            return this.classLoader;
        }
    }
}
