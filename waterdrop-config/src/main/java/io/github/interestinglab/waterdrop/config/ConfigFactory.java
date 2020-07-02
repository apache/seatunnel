/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config;

import io.github.interestinglab.waterdrop.config.impl.ConfigImpl;
import io.github.interestinglab.waterdrop.config.impl.Parseable;

import java.io.File;
import java.io.Reader;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Contains static methods for creating {@link Config} instances.
 *
 * <p>
 * See also {@link ConfigValueFactory} which contains static methods for
 * converting Java values into a {@link ConfigObject}. You can then convert a
 * {@code ConfigObject} into a {@code Config} with {@link ConfigObject#toConfig}.
 *
 * <p>
 * The static methods with "load" in the name do some sort of higher-level
 * operation potentially parsing multiple resources and resolving substitutions,
 * while the ones with "parse" in the name just create a {@link ConfigValue}
 * from a resource and nothing else.
 *
 * <p> You can find an example app and library <a
 * href="https://github.com/lightbend/config/tree/master/examples">on
 * GitHub</a>.  Also be sure to read the <a
 * href="package-summary.html#package_description">package
 * overview</a> which describes the big picture as shown in those
 * examples.
 */
public final class ConfigFactory {
    private static final String STRATEGY_PROPERTY_NAME = "config.strategy";

    private ConfigFactory() {
    }

    /**
     * Loads an application's configuration from the given classpath resource or
     * classpath resource basename, sandwiches it between default reference
     * config and default overrides, and then resolves it. The classpath
     * resource is "raw" (it should have no "/" prefix, and is not made relative
     * to any package, so it's like {@link ClassLoader#getResource} not
     * {@link Class#getResource}).
     *
     * <p>
     * Resources are loaded from the current thread's
     * {@link Thread#getContextClassLoader()}. In general, a library needs its
     * configuration to come from the class loader used to load that library, so
     * the proper "reference.conf" are present.
     *
     * <p>
     * The loaded object will already be resolved (substitutions have already
     * been processed). As a result, if you add more fallbacks then they won't
     * be seen by substitutions. Substitutions are the "${foo.bar}" syntax. If
     * you want to parse additional files or something then you need to use
     * {@link #load(Config)}.
     *
     * <p>
     * To load a standalone resource (without the default reference and default
     * overrides), use {@link #parseResourcesAnySyntax(String)} rather than this
     * method. To load only the reference config use {@link #defaultReference()}
     * and to load only the overrides use {@link #defaultOverrides()}.
     *
     * @param resourceBasename
     *            name (optionally without extension) of a resource on classpath
     * @return configuration for an application relative to context class loader
     */
    public static Config load(String resourceBasename) {
        return load(resourceBasename, ConfigParseOptions.defaults(),
                ConfigResolveOptions.defaults());
    }

    /**
     * Like {@link #load(String)} but uses the supplied class loader instead of
     * the current thread's context class loader.
     * 
     * <p>
     * To load a standalone resource (without the default reference and default
     * overrides), use {@link #parseResourcesAnySyntax(ClassLoader, String)}
     * rather than this method. To load only the reference config use
     * {@link #defaultReference(ClassLoader)} and to load only the overrides use
     * {@link #defaultOverrides(ClassLoader)}.
     * 
     * @param loader class loader to look for resources in
     * @param resourceBasename basename (no .conf/.json/.properties suffix)
     * @return configuration for an application relative to given class loader
     */
    public static Config load(ClassLoader loader, String resourceBasename) {
        return load(resourceBasename, ConfigParseOptions.defaults().setClassLoader(loader),
                ConfigResolveOptions.defaults());
    }

    /**
     * Like {@link #load(String)} but allows you to specify parse and resolve
     * options.
     *
     * @param resourceBasename
     *            the classpath resource name with optional extension
     * @param parseOptions
     *            options to use when parsing the resource
     * @param resolveOptions
     *            options to use when resolving the stack
     * @return configuration for an application
     */
    public static Config load(String resourceBasename, ConfigParseOptions parseOptions,
                              ConfigResolveOptions resolveOptions) {
        ConfigParseOptions withLoader = ensureClassLoader(parseOptions, "load");
        Config appConfig = ConfigFactory.parseResourcesAnySyntax(resourceBasename, withLoader);
        return load(withLoader.getClassLoader(), appConfig, resolveOptions);
    }

    /**
     * Like {@link #load(String,ConfigParseOptions,ConfigResolveOptions)} but
     * has a class loader parameter that overrides any from the
     * {@code ConfigParseOptions}.
     *
     * @param loader
     *            class loader in which to find resources (overrides loader in
     *            parse options)
     * @param resourceBasename
     *            the classpath resource name with optional extension
     * @param parseOptions
     *            options to use when parsing the resource (class loader
     *            overridden)
     * @param resolveOptions
     *            options to use when resolving the stack
     * @return configuration for an application
     */
    public static Config load(ClassLoader loader, String resourceBasename,
                              ConfigParseOptions parseOptions, ConfigResolveOptions resolveOptions) {
        return load(resourceBasename, parseOptions.setClassLoader(loader), resolveOptions);
    }

    private static ClassLoader checkedContextClassLoader(String methodName) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null)
            throw new ConfigException.BugOrBroken("Context class loader is not set for the current thread; "
                    + "if Thread.currentThread().getContextClassLoader() returns null, you must pass a ClassLoader "
                    + "explicitly to ConfigFactory." + methodName);
        else
            return loader;
    }

    private static ConfigParseOptions ensureClassLoader(ConfigParseOptions options, String methodName) {
        if (options.getClassLoader() == null)
            return options.setClassLoader(checkedContextClassLoader(methodName));
        else
            return options;
    }

    /**
     * Assembles a standard configuration using a custom <code>Config</code>
     * object rather than loading "application.conf". The <code>Config</code>
     * object will be sandwiched between the default reference config and
     * default overrides and then resolved.
     *
     * @param config
     *            the application's portion of the configuration
     * @return resolved configuration with overrides and fallbacks added
     */
    public static Config load(Config config) {
        return load(checkedContextClassLoader("load"), config);
    }

    /**
     * Like {@link #load(Config)} but allows you to specify
     * the class loader for looking up resources.
     *
     * @param loader
     *            the class loader to use to find resources
     * @param config
     *            the application's portion of the configuration
     * @return resolved configuration with overrides and fallbacks added
     */
    public static Config load(ClassLoader loader, Config config) {
        return load(loader, config, ConfigResolveOptions.defaults());
    }

    /**
     * Like {@link #load(Config)} but allows you to specify
     * {@link ConfigResolveOptions}.
     *
     * @param config
     *            the application's portion of the configuration
     * @param resolveOptions
     *            options for resolving the assembled config stack
     * @return resolved configuration with overrides and fallbacks added
     */
    public static Config load(Config config, ConfigResolveOptions resolveOptions) {
        return load(checkedContextClassLoader("load"), config, resolveOptions);
    }

    /**
     * Like {@link #load(Config,ConfigResolveOptions)} but allows you to specify
     * a class loader other than the context class loader.
     *
     * @param loader
     *            class loader to use when looking up override and reference
     *            configs
     * @param config
     *            the application's portion of the configuration
     * @param resolveOptions
     *            options for resolving the assembled config stack
     * @return resolved configuration with overrides and fallbacks added
     */
    public static Config load(ClassLoader loader, Config config, ConfigResolveOptions resolveOptions) {
        return defaultOverrides(loader).withFallback(config).withFallback(defaultReference(loader))
                .resolve(resolveOptions);
    }



    /**
     * Loads a default configuration, equivalent to {@link #load(Config)
     * load(defaultApplication())} in most cases. This configuration should be used by
     * libraries and frameworks unless an application provides a different one.
     * <p>
     * This method may return a cached singleton so will not see changes to
     * system properties or config files. (Use {@link #invalidateCaches()} to
     * force it to reload.)
     *
     * @return configuration for an application
     */
    public static Config load() {
        ClassLoader loader = checkedContextClassLoader("load");
        return load(loader);
    }

    /**
     * Like {@link #load()} but allows specifying parse options.
     *
     * @param parseOptions
     *            Options for parsing resources
     * @return configuration for an application
     */
    public static Config load(ConfigParseOptions parseOptions) {
        return load(parseOptions, ConfigResolveOptions.defaults());
    }

    /**
     * Like {@link #load()} but allows specifying a class loader other than the
     * thread's current context class loader.
     *
     * @param loader
     *            class loader for finding resources
     * @return configuration for an application
     */
    public static Config load(final ClassLoader loader) {
        final ConfigParseOptions withLoader = ConfigParseOptions.defaults().setClassLoader(loader);
        return ConfigImpl.computeCachedConfig(loader, "load", new Callable<Config>() {
            @Override
            public Config call() {
                return load(loader, defaultApplication(withLoader));
            }
        });
    }

    /**
     * Like {@link #load()} but allows specifying a class loader other than the
     * thread's current context class loader and also specify parse options.
     *
     * @param loader
     *            class loader for finding resources (overrides any loader in parseOptions)
     * @param parseOptions
     *            Options for parsing resources
     * @return configuration for an application
     */
    public static Config load(ClassLoader loader, ConfigParseOptions parseOptions) {
        return load(parseOptions.setClassLoader(loader));
    }

    /**
     * Like {@link #load()} but allows specifying a class loader other than the
     * thread's current context class loader and also specify resolve options.
     *
     * @param loader
     *            class loader for finding resources
     * @param resolveOptions
     *            options for resolving the assembled config stack
     * @return configuration for an application
     */
    public static Config load(ClassLoader loader, ConfigResolveOptions resolveOptions) {
        return load(loader, ConfigParseOptions.defaults(), resolveOptions);
    }


    /**
     * Like {@link #load()} but allows specifying a class loader other than the
     * thread's current context class loader, parse options, and resolve options.
     *
     * @param loader
     *            class loader for finding resources (overrides any loader in parseOptions)
     * @param parseOptions
     *            Options for parsing resources
     * @param resolveOptions
     *            options for resolving the assembled config stack
     * @return configuration for an application
     */
    public static Config load(ClassLoader loader, ConfigParseOptions parseOptions, ConfigResolveOptions resolveOptions) {
        final ConfigParseOptions withLoader = ensureClassLoader(parseOptions, "load");
        return load(loader, defaultApplication(withLoader), resolveOptions);
    }

    /**
     * Like {@link #load()} but allows specifying parse options and resolve
     * options.
     *
     * @param parseOptions
     *            Options for parsing resources
     * @param resolveOptions
     *            options for resolving the assembled config stack
     * @return configuration for an application
     *
     * @since 1.3.0
     */
    public static Config load(ConfigParseOptions parseOptions, final ConfigResolveOptions resolveOptions) {
        final ConfigParseOptions withLoader = ensureClassLoader(parseOptions, "load");
        return load(defaultApplication(withLoader), resolveOptions);
    }

    /**
     * Obtains the default reference configuration, which is currently created
     * by merging all resources "reference.conf" found on the classpath and
     * overriding the result with system properties. The returned reference
     * configuration will already have substitutions resolved.
     * 
     * <p>
     * Libraries and frameworks should ship with a "reference.conf" in their
     * jar.
     * 
     * <p>
     * The reference config must be looked up in the class loader that contains
     * the libraries that you want to use with this config, so the
     * "reference.conf" for each library can be found. Use
     * {@link #defaultReference(ClassLoader)} if the context class loader is not
     * suitable.
     * 
     * <p>
     * The {@link #load()} methods merge this configuration for you
     * automatically.
     * 
     * <p>
     * Future versions may look for reference configuration in more places. It
     * is not guaranteed that this method <em>only</em> looks at
     * "reference.conf".
     * 
     * @return the default reference config for context class loader
     */
    public static Config defaultReference() {
        return defaultReference(checkedContextClassLoader("defaultReference"));
    }

    /**
     * Like {@link #defaultReference()} but allows you to specify a class loader
     * to use rather than the current context class loader.
     *
     * @param loader class loader to look for resources in
     * @return the default reference config for this class loader
     */
    public static Config defaultReference(ClassLoader loader) {
        return ConfigImpl.defaultReference(loader);
    }

    /**
     * Obtains the default override configuration, which currently consists of
     * system properties. The returned override configuration will already have
     * substitutions resolved.
     *
     * <p>
     * The {@link #load()} methods merge this configuration for you
     * automatically.
     *
     * <p>
     * Future versions may get overrides in more places. It is not guaranteed
     * that this method <em>only</em> uses system properties.
     *
     * @return the default override configuration
     */
    public static Config defaultOverrides() {
        return systemProperties();
    }

    /**
     * Like {@link #defaultOverrides()} but allows you to specify a class loader
     * to use rather than the current context class loader.
     *
     * @param loader class loader to look for resources in
     * @return the default override configuration
     */
    public static Config defaultOverrides(ClassLoader loader) {
        return systemProperties();
    }

    /**
     * Obtains the default application-specific configuration,
     * which defaults to parsing <code>application.conf</code>,
     * <code>application.json</code>, and
     * <code>application.properties</code> on the classpath, but
     * can also be rerouted using the <code>config.file</code>,
     * <code>config.resource</code>, and <code>config.url</code>
     * system properties.
     *
     * <p> The no-arguments {@link #load()} method automatically
     * stacks the {@link #defaultReference()}, {@link
     * #defaultApplication()}, and {@link #defaultOverrides()}
     * configs. You would use <code>defaultApplication()</code>
     * directly only if you're somehow customizing behavior by
     * reimplementing <code>load()</code>.
     *
     * <p>The configuration returned by
     * <code>defaultApplication()</code> will not be resolved
     * already, in contrast to <code>defaultReference()</code> and
     * <code>defaultOverrides()</code>. This is because
     * application.conf would normally be resolved <em>after</em>
     * merging with the reference and override configs.
     *
     * <p>
     * If the system properties <code>config.resource</code>,
     * <code>config.file</code>, or <code>config.url</code> are set, then the
     * classpath resource, file, or URL specified in those properties will be
     * used rather than the default
     * <code>application.{conf,json,properties}</code> classpath resources.
     * These system properties should not be set in code (after all, you can
     * just parse whatever you want manually and then use {@link #load(Config)}
     * if you don't want to use <code>application.conf</code>). The properties
     * are intended for use by the person or script launching the application.
     * For example someone might have a <code>production.conf</code> that
     * include <code>application.conf</code> but then change a couple of values.
     * When launching the app they could specify
     * <code>-Dconfig.resource=production.conf</code> to get production mode.
     *
     * <p>
     * If no system properties are set to change the location of the default
     * configuration, <code>defaultApplication()</code> is equivalent to
     * <code>ConfigFactory.parseResources("application")</code>.
     *
     * @since 1.3.0
     *
     * @return the default application.conf or system-property-configured configuration
     */
    public static Config defaultApplication() {
        return defaultApplication(ConfigParseOptions.defaults());
    }

    /**
     * Like {@link #defaultApplication()} but allows you to specify a class loader
     * to use rather than the current context class loader.
     *
     * @since 1.3.0
     *
     * @param loader class loader to look for resources in
     * @return the default application configuration
     */
    public static Config defaultApplication(ClassLoader loader) {
        return defaultApplication(ConfigParseOptions.defaults().setClassLoader(loader));
    }

    /**
     * Like {@link #defaultApplication()} but allows you to specify parse options.
     *
     * @since 1.3.0
     *
     * @param options the options
     * @return the default application configuration
     */
    public static Config defaultApplication(ConfigParseOptions options) {
        return getConfigLoadingStrategy().parseApplicationConfig(ensureClassLoader(options, "defaultApplication"));
    }

    /**
     * Reloads any cached configs, picking up changes to system properties for
     * example. Because a {@link Config} is immutable, anyone with a reference
     * to the old configs will still have the same outdated objects. However,
     * new calls to {@link #load()} or {@link #defaultOverrides()} or
     * {@link #defaultReference} may return a new object.
     * <p>
     * This method is primarily intended for use in unit tests, for example,
     * that may want to update a system property then confirm that it's used
     * correctly. In many cases, use of this method may indicate there's a
     * better way to set up your code.
     * <p>
     * Caches may be reloaded immediately or lazily; once you call this method,
     * the reload can occur at any time, even during the invalidation process.
     * So FIRST make the changes you'd like the caches to notice, then SECOND
     * call this method to invalidate caches. Don't expect that invalidating,
     * making changes, then calling {@link #load()}, will work. Make changes
     * before you invalidate.
     */
    public static void invalidateCaches() {
        // We rely on this having the side effect that it drops
        // all caches
        ConfigImpl.reloadSystemPropertiesConfig();
        ConfigImpl.reloadEnvVariablesConfig();
    }

    /**
     * Gets an empty configuration. See also {@link #empty(String)} to create an
     * empty configuration with a description, which may improve user-visible
     * error messages.
     *
     * @return an empty configuration
     */
    public static Config empty() {
        return empty(null);
    }

    /**
     * Gets an empty configuration with a description to be used to create a
     * {@link ConfigOrigin} for this <code>Config</code>. The description should
     * be very short and say what the configuration is, like "default settings"
     * or "foo settings" or something. (Presumably you will merge some actual
     * settings into this empty config using {@link Config#withFallback}, making
     * the description more useful.)
     *
     * @param originDescription
     *            description of the config
     * @return an empty configuration
     */
    public static Config empty(String originDescription) {
        return ConfigImpl.emptyConfig(originDescription);
    }

    /**
     * Gets a <code>Config</code> containing the system properties from
     * {@link java.lang.System#getProperties()}, parsed and converted as with
     * {@link #parseProperties}.
     * <p>
     * This method can return a global immutable singleton, so it's preferred
     * over parsing system properties yourself.
     * <p>
     * {@link #load} will include the system properties as overrides already, as
     * will {@link #defaultReference} and {@link #defaultOverrides}.
     *
     * <p>
     * Because this returns a singleton, it will not notice changes to system
     * properties made after the first time this method is called. Use
     * {@link #invalidateCaches()} to force the singleton to reload if you
     * modify system properties.
     *
     * @return system properties parsed into a <code>Config</code>
     */
    public static Config systemProperties() {
        return ConfigImpl.systemPropertiesAsConfig();
    }

    /**
     * Gets a <code>Config</code> containing the system's environment variables.
     * This method can return a global immutable singleton.
     *
     * <p>
     * Environment variables are used as fallbacks when resolving substitutions
     * whether or not this object is included in the config being resolved, so
     * you probably don't need to use this method for most purposes. It can be a
     * nicer API for accessing environment variables than raw
     * {@link java.lang.System#getenv(String)} though, since you can use methods
     * such as {@link Config#getInt}.
     *
     * @return system environment variables parsed into a <code>Config</code>
     */
    public static Config systemEnvironment() {
        return ConfigImpl.envVariablesAsConfig();
    }

    /**
     * Converts a Java {@link java.util.Properties} object to a
     * {@link ConfigObject} using the rules documented in the <a
     * href="https://github.com/lightbend/config/blob/master/HOCON.md">HOCON
     * spec</a>. The keys in the <code>Properties</code> object are split on the
     * period character '.' and treated as paths. The values will all end up as
     * string values. If you have both "a=foo" and "a.b=bar" in your properties
     * file, so "a" is both the object containing "b" and the string "foo", then
     * the string value is dropped.
     *
     * <p>
     * If you want to have <code>System.getProperties()</code> as a
     * ConfigObject, it's better to use the {@link #systemProperties()} method
     * which returns a cached global singleton.
     *
     * @param properties
     *            a Java Properties object
     * @param options
     *            the parse options
     * @return the parsed configuration
     */
    public static Config parseProperties(Properties properties,
                                         ConfigParseOptions options) {
        return Parseable.newProperties(properties, options).parse().toConfig();
    }

    /**
     * Like {@link #parseProperties(Properties, ConfigParseOptions)} but uses default
     * parse options.
     * @param properties
     *            a Java Properties object
     * @return the parsed configuration
     */
    public static Config parseProperties(Properties properties) {
        return parseProperties(properties, ConfigParseOptions.defaults());
    }

    /**
     * Parses a Reader into a Config instance. Does not call
     * {@link Config#resolve} or merge the parsed stream with any
     * other configuration; this method parses a single stream and
     * does nothing else. It does process "include" statements in
     * the parsed stream, and may end up doing other IO due to those
     * statements.
     *
     * @param reader
     *       the reader to parse
     * @param options
     *       parse options to control how the reader is interpreted
     * @return the parsed configuration
     * @throws ConfigException on IO or parse errors
     */
    public static Config parseReader(Reader reader, ConfigParseOptions options) {
        return Parseable.newReader(reader, options).parse().toConfig();
    }

    /**
     * Parses a reader into a Config instance as with
     * {@link #parseReader(Reader,ConfigParseOptions)} but always uses the
     * default parse options.
     *
     * @param reader
     *       the reader to parse
     * @return the parsed configuration
     * @throws ConfigException on IO or parse errors
     */
    public static Config parseReader(Reader reader) {
        return parseReader(reader, ConfigParseOptions.defaults());
    }

    /**
     * Parses a URL into a Config instance. Does not call
     * {@link Config#resolve} or merge the parsed stream with any
     * other configuration; this method parses a single stream and
     * does nothing else. It does process "include" statements in
     * the parsed stream, and may end up doing other IO due to those
     * statements.
     *
     * @param url
     *       the url to parse
     * @param options
     *       parse options to control how the url is interpreted
     * @return the parsed configuration
     * @throws ConfigException on IO or parse errors
     */
    public static Config parseURL(URL url, ConfigParseOptions options) {
        return Parseable.newURL(url, options).parse().toConfig();
    }

    /**
     * Parses a url into a Config instance as with
     * {@link #parseURL(URL,ConfigParseOptions)} but always uses the
     * default parse options.
     *
     * @param url
     *       the url to parse
     * @return the parsed configuration
     * @throws ConfigException on IO or parse errors
     */
    public static Config parseURL(URL url) {
        return parseURL(url, ConfigParseOptions.defaults());
    }

    /**
     * Parses a file into a Config instance. Does not call
     * {@link Config#resolve} or merge the file with any other
     * configuration; this method parses a single file and does
     * nothing else. It does process "include" statements in the
     * parsed file, and may end up doing other IO due to those
     * statements.
     *
     * @param file
     *       the file to parse
     * @param options
     *       parse options to control how the file is interpreted
     * @return the parsed configuration
     * @throws ConfigException on IO or parse errors
     */
    public static Config parseFile(File file, ConfigParseOptions options) {
        return Parseable.newFile(file, options).parse().toConfig();
    }

    /**
     * Parses a file into a Config instance as with
     * {@link #parseFile(File,ConfigParseOptions)} but always uses the
     * default parse options.
     *
     * @param file
     *       the file to parse
     * @return the parsed configuration
     * @throws ConfigException on IO or parse errors
     */
    public static Config parseFile(File file) {
        return parseFile(file, ConfigParseOptions.defaults());
    }

    /**
     * Parses a file with a flexible extension. If the <code>fileBasename</code>
     * already ends in a known extension, this method parses it according to
     * that extension (the file's syntax must match its extension). If the
     * <code>fileBasename</code> does not end in an extension, it parses files
     * with all known extensions and merges whatever is found.
     *
     * <p>
     * In the current implementation, the extension ".conf" forces
     * {@link ConfigSyntax#CONF}, ".json" forces {@link ConfigSyntax#JSON}, and
     * ".properties" forces {@link ConfigSyntax#PROPERTIES}. When merging files,
     * ".conf" falls back to ".json" falls back to ".properties".
     *
     * <p>
     * Future versions of the implementation may add additional syntaxes or
     * additional extensions. However, the ordering (fallback priority) of the
     * three current extensions will remain the same.
     *
     * <p>
     * If <code>options</code> forces a specific syntax, this method only parses
     * files with an extension matching that syntax.
     *
     * <p>
     * If {@link ConfigParseOptions#getAllowMissing options.getAllowMissing()}
     * is true, then no files have to exist; if false, then at least one file
     * has to exist.
     *
     * @param fileBasename
     *            a filename with or without extension
     * @param options
     *            parse options
     * @return the parsed configuration
     */
    public static Config parseFileAnySyntax(File fileBasename,
                                            ConfigParseOptions options) {
        return ConfigImpl.parseFileAnySyntax(fileBasename, options).toConfig();
    }

    /**
     * Like {@link #parseFileAnySyntax(File,ConfigParseOptions)} but always uses
     * default parse options.
     *
     * @param fileBasename
     *            a filename with or without extension
     * @return the parsed configuration
     */
    public static Config parseFileAnySyntax(File fileBasename) {
        return parseFileAnySyntax(fileBasename, ConfigParseOptions.defaults());
    }

    /**
     * Parses all resources on the classpath with the given name and merges them
     * into a single <code>Config</code>.
     *
     * <p>
     * If the resource name does not begin with a "/", it will have the supplied
     * class's package added to it, in the same way as
     * {@link java.lang.Class#getResource}.
     *
     * <p>
     * Duplicate resources with the same name are merged such that ones returned
     * earlier from {@link ClassLoader#getResources} fall back to (have higher
     * priority than) the ones returned later. This implies that resources
     * earlier in the classpath override those later in the classpath when they
     * configure the same setting. However, in practice real applications may
     * not be consistent about classpath ordering, so be careful. It may be best
     * to avoid assuming too much.
     *
     * @param klass
     *            <code>klass.getClassLoader()</code> will be used to load
     *            resources, and non-absolute resource names will have this
     *            class's package added
     * @param resource
     *            resource to look up, relative to <code>klass</code>'s package
     *            or absolute starting with a "/"
     * @param options
     *            parse options
     * @return the parsed configuration
     */
    public static Config parseResources(Class<?> klass, String resource,
                                        ConfigParseOptions options) {
        return Parseable.newResources(klass, resource, options).parse()
                .toConfig();
    }

    /**
     * Like {@link #parseResources(Class, String,ConfigParseOptions)} but always uses
     * default parse options.
     *
     * @param klass
     *            <code>klass.getClassLoader()</code> will be used to load
     *            resources, and non-absolute resource names will have this
     *            class's package added
     * @param resource
     *            resource to look up, relative to <code>klass</code>'s package
     *            or absolute starting with a "/"
     * @return the parsed configuration
     */
    public static Config parseResources(Class<?> klass, String resource) {
        return parseResources(klass, resource, ConfigParseOptions.defaults());
    }

    /**
     * Parses classpath resources with a flexible extension. In general, this
     * method has the same behavior as
     * {@link #parseFileAnySyntax(File,ConfigParseOptions)} but for classpath
     * resources instead, as in {@link #parseResources}.
     *
     * <p>
     * There is a thorny problem with this method, which is that
     * {@link java.lang.ClassLoader#getResources} must be called separately for
     * each possible extension. The implementation ends up with separate lists
     * of resources called "basename.conf" and "basename.json" for example. As a
     * result, the ideal ordering between two files with different extensions is
     * unknown; there is no way to figure out how to merge the two lists in
     * classpath order. To keep it simple, the lists are simply concatenated,
     * with the same syntax priorities as
     * {@link #parseFileAnySyntax(File,ConfigParseOptions) parseFileAnySyntax()}
     * - all ".conf" resources are ahead of all ".json" resources which are
     * ahead of all ".properties" resources.
     *
     * @param klass
     *            class which determines the <code>ClassLoader</code> and the
     *            package for relative resource names
     * @param resourceBasename
     *            a resource name as in {@link java.lang.Class#getResource},
     *            with or without extension
     * @param options
     *            parse options (class loader is ignored in favor of the one
     *            from klass)
     * @return the parsed configuration
     */
    public static Config parseResourcesAnySyntax(Class<?> klass, String resourceBasename,
                                                 ConfigParseOptions options) {
        return ConfigImpl.parseResourcesAnySyntax(klass, resourceBasename,
                options).toConfig();
    }

    /**
     * Like {@link #parseResourcesAnySyntax(Class, String,ConfigParseOptions)}
     * but always uses default parse options.
     *
     * @param klass
     *            <code>klass.getClassLoader()</code> will be used to load
     *            resources, and non-absolute resource names will have this
     *            class's package added
     * @param resourceBasename
     *            a resource name as in {@link java.lang.Class#getResource},
     *            with or without extension
     * @return the parsed configuration
     */
    public static Config parseResourcesAnySyntax(Class<?> klass, String resourceBasename) {
        return parseResourcesAnySyntax(klass, resourceBasename, ConfigParseOptions.defaults());
    }

    /**
     * Parses all resources on the classpath with the given name and merges them
     * into a single <code>Config</code>.
     *
     * <p>
     * This works like {@link java.lang.ClassLoader#getResource}, not like
     * {@link java.lang.Class#getResource}, so the name never begins with a
     * slash.
     *
     * <p>
     * See {@link #parseResources(Class, String,ConfigParseOptions)} for full
     * details.
     *
     * @param loader
     *            will be used to load resources by setting this loader on the
     *            provided options
     * @param resource
     *            resource to look up
     * @param options
     *            parse options (class loader is ignored)
     * @return the parsed configuration
     */
    public static Config parseResources(ClassLoader loader, String resource,
                                        ConfigParseOptions options) {
        return parseResources(resource, options.setClassLoader(loader));
    }

    /**
     * Like {@link #parseResources(ClassLoader, String,ConfigParseOptions)} but always uses
     * default parse options.
     *
     * @param loader
     *            will be used to load resources
     * @param resource
     *            resource to look up in the loader
     * @return the parsed configuration
     */
    public static Config parseResources(ClassLoader loader, String resource) {
        return parseResources(loader, resource, ConfigParseOptions.defaults());
    }

    /**
     * Parses classpath resources with a flexible extension. In general, this
     * method has the same behavior as
     * {@link #parseFileAnySyntax(File,ConfigParseOptions)} but for classpath
     * resources instead, as in
     * {@link #parseResources(ClassLoader, String,ConfigParseOptions)}.
     *
     * <p>
     * {@link #parseResourcesAnySyntax(Class, String,ConfigParseOptions)} differs
     * in the syntax for the resource name, but otherwise see
     * {@link #parseResourcesAnySyntax(Class, String,ConfigParseOptions)} for
     * some details and caveats on this method.
     *
     * @param loader
     *            class loader to look up resources in, will be set on options
     * @param resourceBasename
     *            a resource name as in
     *            {@link java.lang.ClassLoader#getResource}, with or without
     *            extension
     * @param options
     *            parse options (class loader ignored)
     * @return the parsed configuration
     */
    public static Config parseResourcesAnySyntax(ClassLoader loader, String resourceBasename,
                                                 ConfigParseOptions options) {
        return ConfigImpl.parseResourcesAnySyntax(resourceBasename, options.setClassLoader(loader))
                .toConfig();
    }

    /**
     * Like {@link #parseResourcesAnySyntax(ClassLoader, String,ConfigParseOptions)} but always uses
     * default parse options.
     *
     * @param loader
     *            will be used to load resources
     * @param resourceBasename
     *            a resource name as in
     *            {@link java.lang.ClassLoader#getResource}, with or without
     *            extension
     * @return the parsed configuration
     */
    public static Config parseResourcesAnySyntax(ClassLoader loader, String resourceBasename) {
        return parseResourcesAnySyntax(loader, resourceBasename, ConfigParseOptions.defaults());
    }

    /**
     * Like {@link #parseResources(ClassLoader, String,ConfigParseOptions)} but
     * uses thread's current context class loader if none is set in the
     * ConfigParseOptions.
     * @param resource the resource name
     * @param options parse options
     * @return the parsed configuration
     */
    public static Config parseResources(String resource, ConfigParseOptions options) {
        ConfigParseOptions withLoader = ensureClassLoader(options, "parseResources");
        return Parseable.newResources(resource, withLoader).parse().toConfig();
    }

    /**
     * Like {@link #parseResources(ClassLoader, String)} but uses thread's
     * current context class loader.
     * @param resource the resource name
     * @return the parsed configuration
     */
    public static Config parseResources(String resource) {
        return parseResources(resource, ConfigParseOptions.defaults());
    }

    /**
     * Like
     * {@link #parseResourcesAnySyntax(ClassLoader, String,ConfigParseOptions)}
     * but uses thread's current context class loader.
     * @param resourceBasename the resource basename (no file type suffix)
     * @param options parse options
     * @return the parsed configuration
     */
    public static Config parseResourcesAnySyntax(String resourceBasename, ConfigParseOptions options) {
        return ConfigImpl.parseResourcesAnySyntax(resourceBasename, options).toConfig();
    }

    /**
     * Like {@link #parseResourcesAnySyntax(ClassLoader, String)} but uses
     * thread's current context class loader.
     * @param resourceBasename the resource basename (no file type suffix)
     * @return the parsed configuration
     */
    public static Config parseResourcesAnySyntax(String resourceBasename) {
        return parseResourcesAnySyntax(resourceBasename, ConfigParseOptions.defaults());
    }

    /**
     * Parses a string (which should be valid HOCON or JSON by default, or
     * the syntax specified in the options otherwise).
     *
     * @param s string to parse
     * @param options parse options
     * @return the parsed configuration
     */
    public static Config parseString(String s, ConfigParseOptions options) {
        return Parseable.newString(s, options).parse().toConfig();
    }

    /**
     * Parses a string (which should be valid HOCON or JSON).
     *
     * @param s string to parse
     * @return the parsed configuration
     */
    public static Config parseString(String s) {
        return parseString(s, ConfigParseOptions.defaults());
    }

    /**
     * Creates a {@code Config} based on a {@link java.util.Map} from paths to
     * plain Java values. Similar to
     * {@link ConfigValueFactory#fromMap(Map, String)}, except the keys in the
     * map are path expressions, rather than keys; and correspondingly it
     * returns a {@code Config} instead of a {@code ConfigObject}. This is more
     * convenient if you are writing literal maps in code, and less convenient
     * if you are getting your maps from some data source such as a parser.
     *
     * <p>
     * An exception will be thrown (and it is a bug in the caller of the method)
     * if a path is both an object and a value, for example if you had both
     * "a=foo" and "a.b=bar", then "a" is both the string "foo" and the parent
     * object of "b". The caller of this method should ensure that doesn't
     * happen.
     *
     * @param values map from paths to plain Java objects
     * @param originDescription
     *            description of what this map represents, like a filename, or
     *            "default settings" (origin description is used in error
     *            messages)
     * @return the map converted to a {@code Config}
     */
    public static Config parseMap(Map<String, ? extends Object> values,
                                  String originDescription) {
        return ConfigImpl.fromPathMap(values, originDescription).toConfig();
    }

    /**
     * See the other overload of {@link #parseMap(Map, String)} for details,
     * this one just uses a default origin description.
     *
     * @param values map from paths to plain Java values
     * @return the map converted to a {@code Config}
     */
    public static Config parseMap(Map<String, ? extends Object> values) {
        return parseMap(values, null);
    }

    private static ConfigLoadingStrategy getConfigLoadingStrategy() {
        String className = System.getProperties().getProperty(STRATEGY_PROPERTY_NAME);

        if (className != null) {
            try {
                return ConfigLoadingStrategy.class.cast(Class.forName(className).newInstance());
            } catch (Throwable e) {
                throw new ConfigException.BugOrBroken("Failed to load strategy: " + className, e);
            }
        } else {
            return new DefaultConfigLoadingStrategy();
        }
    }
}
