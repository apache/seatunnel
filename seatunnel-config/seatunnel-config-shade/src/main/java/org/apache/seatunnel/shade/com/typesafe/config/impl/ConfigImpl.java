/*
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */

package org.apache.seatunnel.shade.com.typesafe.config.impl;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigIncluder;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigMemorySize;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigOrigin;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseable;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import java.io.File;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Internal implementation detail, not ABI stable, do not touch. For use only by the {@link
 * com.typesafe.config} package.
 */
public class ConfigImpl {

    private static class LoaderCache {
        private Config currentSystemProperties;
        private WeakReference<ClassLoader> currentLoader;
        private Map<String, Config> cache;

        LoaderCache() {
            this.currentSystemProperties = null;
            this.currentLoader = new WeakReference<ClassLoader>(null);
            this.cache = new LinkedHashMap<String, Config>();
        }

        // for now, caching as long as the loader remains the same,
        // drop entire cache if it changes.
        synchronized Config getOrElseUpdate(
                ClassLoader loader, String key, Callable<Config> updater) {
            if (loader != currentLoader.get()) {
                // reset the cache if we start using a different loader
                cache.clear();
                currentLoader = new WeakReference<ClassLoader>(loader);
            }

            Config systemProperties = systemPropertiesAsConfig();
            if (systemProperties != currentSystemProperties) {
                cache.clear();
                currentSystemProperties = systemProperties;
            }

            Config config = cache.get(key);
            if (config == null) {
                try {
                    config = updater.call();
                } catch (RuntimeException e) {
                    throw e; // this will include ConfigException
                } catch (Exception e) {
                    throw new ConfigException.Generic(e.getMessage(), e);
                }
                if (config == null)
                    throw new ConfigException.BugOrBroken("null config from cache updater");
                cache.put(key, config);
            }

            return config;
        }
    }

    private static class LoaderCacheHolder {
        static final LoaderCache cache = new LoaderCache();
    }

    public static Config computeCachedConfig(
            ClassLoader loader, String key, Callable<Config> updater) {
        LoaderCache cache;
        try {
            cache = LoaderCacheHolder.cache;
        } catch (ExceptionInInitializerError e) {
            throw ConfigImplUtil.extractInitializerError(e);
        }
        return cache.getOrElseUpdate(loader, key, updater);
    }

    static class FileNameSource implements SimpleIncluder.NameSource {
        @Override
        public ConfigParseable nameToParseable(String name, ConfigParseOptions parseOptions) {
            return Parseable.newFile(new File(name), parseOptions);
        }
    };

    static class ClasspathNameSource implements SimpleIncluder.NameSource {
        @Override
        public ConfigParseable nameToParseable(String name, ConfigParseOptions parseOptions) {
            return Parseable.newResources(name, parseOptions);
        }
    };

    static class ClasspathNameSourceWithClass implements SimpleIncluder.NameSource {
        private final Class<?> klass;

        public ClasspathNameSourceWithClass(Class<?> klass) {
            this.klass = klass;
        }

        @Override
        public ConfigParseable nameToParseable(String name, ConfigParseOptions parseOptions) {
            return Parseable.newResources(klass, name, parseOptions);
        }
    };

    public static ConfigObject parseResourcesAnySyntax(
            Class<?> klass, String resourceBasename, ConfigParseOptions baseOptions) {
        SimpleIncluder.NameSource source = new ClasspathNameSourceWithClass(klass);
        return SimpleIncluder.fromBasename(source, resourceBasename, baseOptions);
    }

    public static ConfigObject parseResourcesAnySyntax(
            String resourceBasename, ConfigParseOptions baseOptions) {
        SimpleIncluder.NameSource source = new ClasspathNameSource();
        return SimpleIncluder.fromBasename(source, resourceBasename, baseOptions);
    }

    public static ConfigObject parseFileAnySyntax(File basename, ConfigParseOptions baseOptions) {
        SimpleIncluder.NameSource source = new FileNameSource();
        return SimpleIncluder.fromBasename(source, basename.getPath(), baseOptions);
    }

    static AbstractConfigObject emptyObject(String originDescription) {
        ConfigOrigin origin =
                originDescription != null ? SimpleConfigOrigin.newSimple(originDescription) : null;
        return emptyObject(origin);
    }

    public static Config emptyConfig(String originDescription) {
        return emptyObject(originDescription).toConfig();
    }

    static AbstractConfigObject empty(ConfigOrigin origin) {
        return emptyObject(origin);
    }

    // default origin for values created with fromAnyRef and no origin specified
    private static final ConfigOrigin defaultValueOrigin =
            SimpleConfigOrigin.newSimple("hardcoded value");
    private static final ConfigBoolean defaultTrueValue =
            new ConfigBoolean(defaultValueOrigin, true);
    private static final ConfigBoolean defaultFalseValue =
            new ConfigBoolean(defaultValueOrigin, false);
    private static final ConfigNull defaultNullValue = new ConfigNull(defaultValueOrigin);
    private static final SimpleConfigList defaultEmptyList =
            new SimpleConfigList(defaultValueOrigin, Collections.<AbstractConfigValue>emptyList());
    private static final SimpleConfigObject defaultEmptyObject =
            SimpleConfigObject.empty(defaultValueOrigin);

    private static SimpleConfigList emptyList(ConfigOrigin origin) {
        if (origin == null || origin == defaultValueOrigin) return defaultEmptyList;
        else return new SimpleConfigList(origin, Collections.<AbstractConfigValue>emptyList());
    }

    private static AbstractConfigObject emptyObject(ConfigOrigin origin) {
        // we want null origin to go to SimpleConfigObject.empty() to get the
        // origin "empty config" rather than "hardcoded value"
        if (origin == defaultValueOrigin) return defaultEmptyObject;
        else return SimpleConfigObject.empty(origin);
    }

    private static ConfigOrigin valueOrigin(String originDescription) {
        if (originDescription == null) return defaultValueOrigin;
        else return SimpleConfigOrigin.newSimple(originDescription);
    }

    public static ConfigValue fromAnyRef(Object object, String originDescription) {
        ConfigOrigin origin = valueOrigin(originDescription);
        return fromAnyRef(object, origin, FromMapMode.KEYS_ARE_KEYS);
    }

    public static ConfigObject fromPathMap(
            Map<String, ? extends Object> pathMap, String originDescription) {
        ConfigOrigin origin = valueOrigin(originDescription);
        return (ConfigObject) fromAnyRef(pathMap, origin, FromMapMode.KEYS_ARE_PATHS);
    }

    static AbstractConfigValue fromAnyRef(Object object, ConfigOrigin origin, FromMapMode mapMode) {
        if (origin == null) throw new ConfigException.BugOrBroken("origin not supposed to be null");

        if (object == null) {
            if (origin != defaultValueOrigin) return new ConfigNull(origin);
            else return defaultNullValue;
        } else if (object instanceof AbstractConfigValue) {
            return (AbstractConfigValue) object;
        } else if (object instanceof Boolean) {
            if (origin != defaultValueOrigin) {
                return new ConfigBoolean(origin, (Boolean) object);
            } else if ((Boolean) object) {
                return defaultTrueValue;
            } else {
                return defaultFalseValue;
            }
        } else if (object instanceof String) {
            return new ConfigString.Quoted(origin, (String) object);
        } else if (object instanceof Number) {
            // here we always keep the same type that was passed to us,
            // rather than figuring out if a Long would fit in an Int
            // or a Double has no fractional part. i.e. deliberately
            // not using ConfigNumber.newNumber() when we have a
            // Double, Integer, or Long.
            if (object instanceof Double) {
                return new ConfigDouble(origin, (Double) object, null);
            } else if (object instanceof Integer) {
                return new ConfigInt(origin, (Integer) object, null);
            } else if (object instanceof Long) {
                return new ConfigLong(origin, (Long) object, null);
            } else {
                return ConfigNumber.newNumber(origin, ((Number) object).doubleValue(), null);
            }
        } else if (object instanceof Duration) {
            return new ConfigLong(origin, ((Duration) object).toMillis(), null);
        } else if (object instanceof Map) {
            if (((Map<?, ?>) object).isEmpty()) return emptyObject(origin);

            if (mapMode == FromMapMode.KEYS_ARE_KEYS) {
                Map<String, AbstractConfigValue> values =
                        new LinkedHashMap<String, AbstractConfigValue>();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                    Object key = entry.getKey();
                    if (!(key instanceof String))
                        throw new ConfigException.BugOrBroken(
                                "bug in method caller: not valid to create ConfigObject from map with non-String key: "
                                        + key);
                    AbstractConfigValue value = fromAnyRef(entry.getValue(), origin, mapMode);
                    values.put((String) key, value);
                }

                return new SimpleConfigObject(origin, values);
            } else {
                return PropertiesParser.fromPathMap(origin, (Map<?, ?>) object);
            }
        } else if (object instanceof Iterable) {
            Iterator<?> i = ((Iterable<?>) object).iterator();
            if (!i.hasNext()) return emptyList(origin);

            List<AbstractConfigValue> values = new ArrayList<AbstractConfigValue>();
            while (i.hasNext()) {
                AbstractConfigValue v = fromAnyRef(i.next(), origin, mapMode);
                values.add(v);
            }

            return new SimpleConfigList(origin, values);
        } else if (object instanceof ConfigMemorySize) {
            return new ConfigLong(origin, ((ConfigMemorySize) object).toBytes(), null);
        } else {
            throw new ConfigException.BugOrBroken(
                    "bug in method caller: not valid to create ConfigValue from: " + object);
        }
    }

    private static class DefaultIncluderHolder {
        static final ConfigIncluder defaultIncluder = new SimpleIncluder(null);
    }

    static ConfigIncluder defaultIncluder() {
        try {
            return DefaultIncluderHolder.defaultIncluder;
        } catch (ExceptionInInitializerError e) {
            throw ConfigImplUtil.extractInitializerError(e);
        }
    }

    private static Properties getSystemProperties() {
        // Avoid ConcurrentModificationException due to parallel setting of system properties by
        // copying properties
        final Properties systemProperties = System.getProperties();
        final Properties systemPropertiesCopy = new Properties();
        synchronized (systemProperties) {
            systemPropertiesCopy.putAll(systemProperties);
        }
        return systemPropertiesCopy;
    }

    private static AbstractConfigObject loadSystemProperties() {
        return (AbstractConfigObject)
                Parseable.newProperties(
                                getSystemProperties(),
                                ConfigParseOptions.defaults()
                                        .setOriginDescription("system properties"))
                        .parse();
    }

    private static class SystemPropertiesHolder {
        // this isn't final due to the reloadSystemPropertiesConfig() hack below
        static volatile AbstractConfigObject systemProperties = loadSystemProperties();
    }

    static AbstractConfigObject systemPropertiesAsConfigObject() {
        try {
            return SystemPropertiesHolder.systemProperties;
        } catch (ExceptionInInitializerError e) {
            throw ConfigImplUtil.extractInitializerError(e);
        }
    }

    public static Config systemPropertiesAsConfig() {
        return systemPropertiesAsConfigObject().toConfig();
    }

    public static void reloadSystemPropertiesConfig() {
        // ConfigFactory.invalidateCaches() relies on this having the side
        // effect that it drops all caches
        SystemPropertiesHolder.systemProperties = loadSystemProperties();
    }

    private static AbstractConfigObject loadEnvVariables() {
        return PropertiesParser.fromStringMap(newSimpleOrigin("env variables"), System.getenv());
    }

    private static class EnvVariablesHolder {
        static volatile AbstractConfigObject envVariables = loadEnvVariables();
    }

    static AbstractConfigObject envVariablesAsConfigObject() {
        try {
            return EnvVariablesHolder.envVariables;
        } catch (ExceptionInInitializerError e) {
            throw ConfigImplUtil.extractInitializerError(e);
        }
    }

    public static Config envVariablesAsConfig() {
        return envVariablesAsConfigObject().toConfig();
    }

    public static void reloadEnvVariablesConfig() {
        // ConfigFactory.invalidateCaches() relies on this having the side
        // effect that it drops all caches
        EnvVariablesHolder.envVariables = loadEnvVariables();
    }

    public static Config defaultReference(final ClassLoader loader) {
        return computeCachedConfig(
                loader,
                "defaultReference",
                new Callable<Config>() {
                    @Override
                    public Config call() {
                        Config unresolvedResources =
                                Parseable.newResources(
                                                "reference.conf",
                                                ConfigParseOptions.defaults()
                                                        .setClassLoader(loader))
                                        .parse()
                                        .toConfig();
                        return systemPropertiesAsConfig()
                                .withFallback(unresolvedResources)
                                .resolve();
                    }
                });
    }

    private static class DebugHolder {
        private static String LOADS = "loads";
        private static String SUBSTITUTIONS = "substitutions";

        private static Map<String, Boolean> loadDiagnostics() {
            Map<String, Boolean> result = new LinkedHashMap<String, Boolean>();
            result.put(LOADS, false);
            result.put(SUBSTITUTIONS, false);

            // People do -Dconfig.trace=foo,bar to enable tracing of different things
            String s = System.getProperty("config.trace");
            if (s == null) {
                return result;
            } else {
                String[] keys = s.split(",");
                for (String k : keys) {
                    if (k.equals(LOADS)) {
                        result.put(LOADS, true);
                    } else if (k.equals(SUBSTITUTIONS)) {
                        result.put(SUBSTITUTIONS, true);
                    } else {
                        System.err.println(
                                "config.trace property contains unknown trace topic '" + k + "'");
                    }
                }
                return result;
            }
        }

        private static final Map<String, Boolean> diagnostics = loadDiagnostics();

        private static final boolean traceLoadsEnabled = diagnostics.get(LOADS);
        private static final boolean traceSubstitutionsEnabled = diagnostics.get(SUBSTITUTIONS);

        static boolean traceLoadsEnabled() {
            return traceLoadsEnabled;
        }

        static boolean traceSubstitutionsEnabled() {
            return traceSubstitutionsEnabled;
        }
    }

    public static boolean traceLoadsEnabled() {
        try {
            return DebugHolder.traceLoadsEnabled();
        } catch (ExceptionInInitializerError e) {
            throw ConfigImplUtil.extractInitializerError(e);
        }
    }

    public static boolean traceSubstitutionsEnabled() {
        try {
            return DebugHolder.traceSubstitutionsEnabled();
        } catch (ExceptionInInitializerError e) {
            throw ConfigImplUtil.extractInitializerError(e);
        }
    }

    public static void trace(String message) {
        System.err.println(message);
    }

    public static void trace(int indentLevel, String message) {
        while (indentLevel > 0) {
            System.err.print("  ");
            indentLevel -= 1;
        }
        System.err.println(message);
    }

    // the basic idea here is to add the "what" and have a canonical
    // toplevel error message. the "original" exception may however have extra
    // detail about what happened. call this if you have a better "what" than
    // further down on the stack.
    static ConfigException.NotResolved improveNotResolved(
            Path what, ConfigException.NotResolved original) {
        String newMessage =
                what.render()
                        + " has not been resolved, you need to call Config#resolve(),"
                        + " see API docs for Config#resolve()";
        if (newMessage.equals(original.getMessage())) return original;
        else return new ConfigException.NotResolved(newMessage, original);
    }

    public static ConfigOrigin newSimpleOrigin(String description) {
        if (description == null) {
            return defaultValueOrigin;
        } else {
            return SimpleConfigOrigin.newSimple(description);
        }
    }

    public static ConfigOrigin newFileOrigin(String filename) {
        return SimpleConfigOrigin.newFile(filename);
    }

    public static ConfigOrigin newURLOrigin(URL url) {
        return SimpleConfigOrigin.newURL(url);
    }
}
