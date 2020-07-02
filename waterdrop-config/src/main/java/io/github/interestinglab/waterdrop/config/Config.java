/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * An immutable map from config paths to config values. Paths are dot-separated
 * expressions such as <code>foo.bar.baz</code>. Values are as in JSON
 * (booleans, strings, numbers, lists, or objects), represented by
 * {@link ConfigValue} instances. Values accessed through the
 * <code>Config</code> interface are never null.
 * 
 * <p>
 * {@code Config} is an immutable object and thus safe to use from multiple
 * threads. There's never a need for "defensive copies."
 * 
 * <p>
 * Fundamental operations on a {@code Config} include getting configuration
 * values, <em>resolving</em> substitutions with {@link Config#resolve()}, and
 * merging configs using {@link Config#withFallback(ConfigMergeable)}.
 * 
 * <p>
 * All operations return a new immutable {@code Config} rather than modifying
 * the original instance.
 * 
 * <p>
 * <strong>Examples</strong>
 * 
 * <p>
 * You can find an example app and library <a
 * href="https://github.com/lightbend/config/tree/master/examples">on
 * GitHub</a>. Also be sure to read the <a
 * href="package-summary.html#package_description">package overview</a> which
 * describes the big picture as shown in those examples.
 * 
 * <p>
 * <strong>Paths, keys, and Config vs. ConfigObject</strong>
 * 
 * <p>
 * <code>Config</code> is a view onto a tree of {@link ConfigObject}; the
 * corresponding object tree can be found through {@link Config#root()}.
 * <code>ConfigObject</code> is a map from config <em>keys</em>, rather than
 * paths, to config values. Think of <code>ConfigObject</code> as a JSON object
 * and <code>Config</code> as a configuration API.
 * 
 * <p>
 * The API tries to consistently use the terms "key" and "path." A key is a key
 * in a JSON object; it's just a string that's the key in a map. A "path" is a
 * parseable expression with a syntax and it refers to a series of keys. Path
 * expressions are described in the <a
 * href="https://github.com/lightbend/config/blob/master/HOCON.md">spec for
 * Human-Optimized Config Object Notation</a>. In brief, a path is
 * period-separated so "a.b.c" looks for key c in object b in object a in the
 * root object. Sometimes double quotes are needed around special characters in
 * path expressions.
 * 
 * <p>
 * The API for a {@code Config} is in terms of path expressions, while the API
 * for a {@code ConfigObject} is in terms of keys. Conceptually, {@code Config}
 * is a one-level map from <em>paths</em> to values, while a
 * {@code ConfigObject} is a tree of nested maps from <em>keys</em> to values.
 * 
 * <p>
 * Use {@link ConfigUtil#joinPath} and {@link ConfigUtil#splitPath} to convert
 * between path expressions and individual path elements (keys).
 * 
 * <p>
 * Another difference between {@code Config} and {@code ConfigObject} is that
 * conceptually, {@code ConfigValue}s with a {@link ConfigValue#valueType()
 * valueType()} of {@link ConfigValueType#NULL NULL} exist in a
 * {@code ConfigObject}, while a {@code Config} treats null values as if they
 * were missing. (With the exception of two methods: {@link Config#hasPathOrNull}
 * and {@link Config#getIsNull} let you detect <code>null</code> values.)
 *
 * <p>
 * <strong>Getting configuration values</strong>
 * 
 * <p>
 * The "getters" on a {@code Config} all work in the same way. They never return
 * null, nor do they return a {@code ConfigValue} with
 * {@link ConfigValue#valueType() valueType()} of {@link ConfigValueType#NULL
 * NULL}. Instead, they throw {@link ConfigException.Missing} if the value is
 * completely absent or set to null. If the value is set to null, a subtype of
 * {@code ConfigException.Missing} called {@link ConfigException.Null} will be
 * thrown. {@link ConfigException.WrongType} will be thrown anytime you ask for
 * a type and the value has an incompatible type. Reasonable type conversions
 * are performed for you though.
 * 
 * <p>
 * <strong>Iteration</strong>
 * 
 * <p>
 * If you want to iterate over the contents of a {@code Config}, you can get its
 * {@code ConfigObject} with {@link #root()}, and then iterate over the
 * {@code ConfigObject} (which implements <code>java.util.Map</code>). Or, you
 * can use {@link #entrySet()} which recurses the object tree for you and builds
 * up a <code>Set</code> of all path-value pairs where the value is not null.
 * 
 * <p>
 * <strong>Resolving substitutions</strong>
 * 
 * <p>
 * <em>Substitutions</em> are the <code>${foo.bar}</code> syntax in config
 * files, described in the <a href=
 * "https://github.com/lightbend/config/blob/master/HOCON.md#substitutions"
 * >specification</a>. Resolving substitutions replaces these references with real
 * values.
 * 
 * <p>
 * Before using a {@code Config} it's necessary to call {@link Config#resolve()}
 * to handle substitutions (though {@link ConfigFactory#load()} and similar
 * methods will do the resolve for you already).
 * 
 * <p>
 * <strong>Merging</strong>
 * 
 * <p>
 * The full <code>Config</code> for your application can be constructed using
 * the associative operation {@link Config#withFallback(ConfigMergeable)}. If
 * you use {@link ConfigFactory#load()} (recommended), it merges system
 * properties over the top of <code>application.conf</code> over the top of
 * <code>reference.conf</code>, using <code>withFallback</code>. You can add in
 * additional sources of configuration in the same way (usually, custom layers
 * should go either just above or just below <code>application.conf</code>,
 * keeping <code>reference.conf</code> at the bottom and system properties at
 * the top).
 * 
 * <p>
 * <strong>Serialization</strong>
 * 
 * <p>
 * Convert a <code>Config</code> to a JSON or HOCON string by calling
 * {@link ConfigObject#render()} on the root object,
 * <code>myConfig.root().render()</code>. There's also a variant
 * {@link ConfigObject#render(ConfigRenderOptions)} which allows you to control
 * the format of the rendered string. (See {@link ConfigRenderOptions}.) Note
 * that <code>Config</code> does not remember the formatting of the original
 * file, so if you load, modify, and re-save a config file, it will be
 * substantially reformatted.
 * 
 * <p>
 * As an alternative to {@link ConfigObject#render()}, the
 * <code>toString()</code> method produces a debug-output-oriented
 * representation (which is not valid JSON).
 * 
 * <p>
 * Java serialization is supported as well for <code>Config</code> and all
 * subtypes of <code>ConfigValue</code>.
 * 
 * <p>
 * <strong>This is an interface but don't implement it yourself</strong>
 * 
 * <p>
 * <em>Do not implement {@code Config}</em>; it should only be implemented by
 * the config library. Arbitrary implementations will not work because the
 * library internals assume a specific concrete implementation. Also, this
 * interface is likely to grow new methods over time, so third-party
 * implementations will break.
 */
public interface Config extends ConfigMergeable {
    /**
     * Gets the {@code Config} as a tree of {@link ConfigObject}. This is a
     * constant-time operation (it is not proportional to the number of values
     * in the {@code Config}).
     *
     * @return the root object in the configuration
     */
    ConfigObject root();

    /**
     * Gets the origin of the {@code Config}, which may be a file, or a file
     * with a line number, or just a descriptive phrase.
     *
     * @return the origin of the {@code Config} for use in error messages
     */
    ConfigOrigin origin();

    @Override
    Config withFallback(ConfigMergeable other);

    /**
     * Returns a replacement config with all substitutions (the
     * <code>${foo.bar}</code> syntax, see <a
     * href="https://github.com/lightbend/config/blob/master/HOCON.md">the
     * spec</a>) resolved. Substitutions are looked up using this
     * <code>Config</code> as the root object, that is, a substitution
     * <code>${foo.bar}</code> will be replaced with the result of
     * <code>getValue("foo.bar")</code>.
     * 
     * <p>
     * This method uses {@link ConfigResolveOptions#defaults()}, there is
     * another variant {@link Config#resolve(ConfigResolveOptions)} which lets
     * you specify non-default options.
     * 
     * <p>
     * A given {@link Config} must be resolved before using it to retrieve
     * config values, but ideally should be resolved one time for your entire
     * stack of fallbacks (see {@link Config#withFallback}). Otherwise, some
     * substitutions that could have resolved with all fallbacks available may
     * not resolve, which will be potentially confusing for your application's
     * users.
     * 
     * <p>
     * <code>resolve()</code> should be invoked on root config objects, rather
     * than on a subtree (a subtree is the result of something like
     * <code>config.getConfig("foo")</code>). The problem with
     * <code>resolve()</code> on a subtree is that substitutions are relative to
     * the root of the config and the subtree will have no way to get values
     * from the root. For example, if you did
     * <code>config.getConfig("foo").resolve()</code> on the below config file,
     * it would not work:
     * 
     * <pre>
     *   common-value = 10
     *   foo {
     *      whatever = ${common-value}
     *   }
     * </pre>
     * 
     * <p>
     * Many methods on {@link ConfigFactory} such as
     * {@link ConfigFactory#load()} automatically resolve the loaded
     * <code>Config</code> on the loaded stack of config files.
     * 
     * <p>
     * Resolving an already-resolved config is a harmless no-op, but again, it
     * is best to resolve an entire stack of fallbacks (such as all your config
     * files combined) rather than resolving each one individually.
     * 
     * @return an immutable object with substitutions resolved
     * @throws ConfigException.UnresolvedSubstitution
     *             if any substitutions refer to nonexistent paths
     * @throws ConfigException
     *             some other config exception if there are other problems
     */
    Config resolve();

    /**
     * Like {@link Config#resolve()} but allows you to specify non-default
     * options.
     *
     * @param options
     *            resolve options
     * @return the resolved <code>Config</code> (may be only partially resolved if options are set to allow unresolved)
     */
    Config resolve(ConfigResolveOptions options);

    /**
     * Checks whether the config is completely resolved. After a successful call
     * to {@link Config#resolve()} it will be completely resolved, but after
     * calling {@link Config#resolve(ConfigResolveOptions)} with
     * <code>allowUnresolved</code> set in the options, it may or may not be
     * completely resolved. A newly-loaded config may or may not be completely
     * resolved depending on whether there were substitutions present in the
     * file.
     * 
     * @return true if there are no unresolved substitutions remaining in this
     *         configuration.
     * @since 1.2.0
     */
    boolean isResolved();

    /**
     * Like {@link Config#resolve()} except that substitution values are looked
     * up in the given source, rather than in this instance. This is a
     * special-purpose method which doesn't make sense to use in most cases;
     * it's only needed if you're constructing some sort of app-specific custom
     * approach to configuration. The more usual approach if you have a source
     * of substitution values would be to merge that source into your config
     * stack using {@link Config#withFallback} and then resolve.
     * <p>
     * Note that this method does NOT look in this instance for substitution
     * values. If you want to do that, you could either merge this instance into
     * your value source using {@link Config#withFallback}, or you could resolve
     * multiple times with multiple sources (using
     * {@link ConfigResolveOptions#setAllowUnresolved(boolean)} so the partial
     * resolves don't fail).
     * 
     * @param source
     *            configuration to pull values from
     * @return an immutable object with substitutions resolved
     * @throws ConfigException.UnresolvedSubstitution
     *             if any substitutions refer to paths which are not in the
     *             source
     * @throws ConfigException
     *             some other config exception if there are other problems
     * @since 1.2.0
     */
    Config resolveWith(Config source);

    /**
     * Like {@link Config#resolveWith(Config)} but allows you to specify
     * non-default options.
     * 
     * @param source
     *            source configuration to pull values from
     * @param options
     *            resolve options
     * @return the resolved <code>Config</code> (may be only partially resolved
     *         if options are set to allow unresolved)
     * @since 1.2.0
     */
    Config resolveWith(Config source, ConfigResolveOptions options);

    /**
     * Validates this config against a reference config, throwing an exception
     * if it is invalid. The purpose of this method is to "fail early" with a
     * comprehensive list of problems; in general, anything this method can find
     * would be detected later when trying to use the config, but it's often
     * more user-friendly to fail right away when loading the config.
     *
     * <p>
     * Using this method is always optional, since you can "fail late" instead.
     *
     * <p>
     * You must restrict validation to paths you "own" (those whose meaning are
     * defined by your code module). If you validate globally, you may trigger
     * errors about paths that happen to be in the config but have nothing to do
     * with your module. It's best to allow the modules owning those paths to
     * validate them. Also, if every module validates only its own stuff, there
     * isn't as much redundant work being done.
     *
     * <p>
     * If no paths are specified in <code>checkValid()</code>'s parameter list,
     * validation is for the entire config.
     *
     * <p>
     * If you specify paths that are not in the reference config, those paths
     * are ignored. (There's nothing to validate.)
     *
     * <p>
     * Here's what validation involves:
     *
     * <ul>
     * <li>All paths found in the reference config must be present in this
     * config or an exception will be thrown.
     * <li>
     * Some changes in type from the reference config to this config will cause
     * an exception to be thrown. Not all potential type problems are detected,
     * in particular it's assumed that strings are compatible with everything
     * except objects and lists. This is because string types are often "really"
     * some other type (system properties always start out as strings, or a
     * string like "5ms" could be used with {@link #getMilliseconds}). Also,
     * it's allowed to set any type to null or override null with any type.
     * <li>
     * Any unresolved substitutions in this config will cause a validation
     * failure; both the reference config and this config should be resolved
     * before validation. If the reference config is unresolved, it's a bug in
     * the caller of this method.
     * </ul>
     *
     * <p>
     * If you want to allow a certain setting to have a flexible type (or
     * otherwise want validation to be looser for some settings), you could
     * either remove the problematic setting from the reference config provided
     * to this method, or you could intercept the validation exception and
     * screen out certain problems. Of course, this will only work if all other
     * callers of this method are careful to restrict validation to their own
     * paths, as they should be.
     *
     * <p>
     * If validation fails, the thrown exception contains a list of all problems
     * found. See {@link ConfigException.ValidationFailed#problems}. The
     * exception's <code>getMessage()</code> will have all the problems
     * concatenated into one huge string, as well.
     *
     * <p>
     * Again, <code>checkValid()</code> can't guess every domain-specific way a
     * setting can be invalid, so some problems may arise later when attempting
     * to use the config. <code>checkValid()</code> is limited to reporting
     * generic, but common, problems such as missing settings and blatant type
     * incompatibilities.
     *
     * @param reference
     *            a reference configuration
     * @param restrictToPaths
     *            only validate values underneath these paths that your code
     *            module owns and understands
     * @throws ConfigException.ValidationFailed
     *             if there are any validation issues
     * @throws ConfigException.NotResolved
     *             if this config is not resolved
     * @throws ConfigException.BugOrBroken
     *             if the reference config is unresolved or caller otherwise
     *             misuses the API
     */
    void checkValid(Config reference, String... restrictToPaths);

    /**
     * Checks whether a value is present and non-null at the given path. This
     * differs in two ways from {@code Map.containsKey()} as implemented by
     * {@link ConfigObject}: it looks for a path expression, not a key; and it
     * returns false for null values, while {@code containsKey()} returns true
     * indicating that the object contains a null value for the key.
     * 
     * <p>
     * If a path exists according to {@link #hasPath(String)}, then
     * {@link #getValue(String)} will never throw an exception. However, the
     * typed getters, such as {@link #getInt(String)}, will still throw if the
     * value is not convertible to the requested type.
     * 
     * <p>
     * Note that path expressions have a syntax and sometimes require quoting
     * (see {@link ConfigUtil#joinPath} and {@link ConfigUtil#splitPath}).
     * 
     * @param path
     *            the path expression
     * @return true if a non-null value is present at the path
     * @throws ConfigException.BadPath
     *             if the path expression is invalid
     */
    boolean hasPath(String path);

    /**
     * Checks whether a value is present at the given path, even
     * if the value is null. Most of the getters on
     * <code>Config</code> will throw if you try to get a null
     * value, so if you plan to call {@link #getValue(String)},
     * {@link #getInt(String)}, or another getter you may want to
     * use plain {@link #hasPath(String)} rather than this method.
     *
     * <p>
     * To handle all three cases (unset, null, and a non-null value)
     * the code might look like:
     * <pre><code>
     * if (config.hasPathOrNull(path)) {
     *     if (config.getIsNull(path)) {
     *        // handle null setting
     *     } else {
     *        // get and use non-null setting
     *     }
     * } else {
     *     // handle entirely unset path
     * }
     * </code></pre>
     *
     * <p> However, the usual thing is to allow entirely unset
     * paths to be a bug that throws an exception (because you set
     * a default in your <code>reference.conf</code>), so in that
     * case it's OK to call {@link #getIsNull(String)} without
     * checking <code>hasPathOrNull</code> first.
     *
     * <p>
     * Note that path expressions have a syntax and sometimes require quoting
     * (see {@link ConfigUtil#joinPath} and {@link ConfigUtil#splitPath}).
     *
     * @param path
     *            the path expression
     * @return true if a value is present at the path, even if the value is null
     * @throws ConfigException.BadPath
     *             if the path expression is invalid
     */
    boolean hasPathOrNull(String path);

    /**
     * Returns true if the {@code Config}'s root object contains no key-value
     * pairs.
     *
     * @return true if the configuration is empty
     */
    boolean isEmpty();

    /**
     * Returns the set of path-value pairs, excluding any null values, found by
     * recursing {@link #root() the root object}. Note that this is very
     * different from <code>root().entrySet()</code> which returns the set of
     * immediate-child keys in the root object and includes null values.
     * <p>
     * Entries contain <em>path expressions</em> meaning there may be quoting
     * and escaping involved. Parse path expressions with
     * {@link ConfigUtil#splitPath}.
     * <p>
     * Because a <code>Config</code> is conceptually a single-level map from
     * paths to values, there will not be any {@link ConfigObject} values in the
     * entries (that is, all entries represent leaf nodes). Use
     * {@link ConfigObject} rather than <code>Config</code> if you want a tree.
     * (OK, this is a slight lie: <code>Config</code> entries may contain
     * {@link ConfigList} and the lists may contain objects. But no objects are
     * directly included as entry values.)
     * 
     * @return set of paths with non-null values, built up by recursing the
     *         entire tree of {@link ConfigObject} and creating an entry for
     *         each leaf value.
     */
    Set<Map.Entry<String, ConfigValue>> entrySet();

    /**
     * Checks whether a value is set to null at the given path,
     * but throws an exception if the value is entirely
     * unset. This method will not throw if {@link
     * #hasPathOrNull(String)} returned true for the same path, so
     * to avoid any possible exception check
     * <code>hasPathOrNull()</code> first.  However, an exception
     * for unset paths will usually be the right thing (because a
     * <code>reference.conf</code> should exist that has the path
     * set, the path should never be unset unless something is
     * broken).
     *
     * <p>
     * Note that path expressions have a syntax and sometimes require quoting
     * (see {@link ConfigUtil#joinPath} and {@link ConfigUtil#splitPath}).
     *
     * @param path
     *            the path expression
     * @return true if the value exists and is null, false if it
     * exists and is not null
     * @throws ConfigException.BadPath
     *             if the path expression is invalid
     * @throws ConfigException.Missing
     *             if value is not set at all
     */
    boolean getIsNull(String path);

    /**
     *
     * @param path
     *            path expression
     * @return the boolean value at the requested path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to boolean
     */
    boolean getBoolean(String path);

    /**
     * @param path
     *            path expression
     * @return the numeric value at the requested path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a number
     */
    Number getNumber(String path);

    /**
     * Gets the integer at the given path. If the value at the
     * path has a fractional (floating point) component, it
     * will be discarded and only the integer part will be
     * returned (it works like a "narrowing primitive conversion"
     * in the Java language specification).
     *
     * @param path
     *            path expression
     * @return the 32-bit integer value at the requested path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to an int (for example it is out
     *             of range, or it's a boolean value)
     */
    int getInt(String path);

    /**
     * Gets the long integer at the given path.  If the value at
     * the path has a fractional (floating point) component, it
     * will be discarded and only the integer part will be
     * returned (it works like a "narrowing primitive conversion"
     * in the Java language specification).
     *
     * @param path
     *            path expression
     * @return the 64-bit long value at the requested path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a long
     */
    long getLong(String path);

    /**
     * @param path
     *            path expression
     * @return the floating-point value at the requested path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a double
     */
    double getDouble(String path);

    /**
     * @param path
     *            path expression
     * @return the string value at the requested path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a string
     */
    String getString(String path);

    /**
     * @param enumClass
     *            an enum class
     * @param <T>
     *            a generic denoting a specific type of enum
     * @param path
     *            path expression
     * @return the {@code Enum} value at the requested path
     *              of the requested enum class
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to an Enum
     */
    public <T extends Enum<T>> T getEnum(Class<T> enumClass, String path);

    /**
     * @param path
     *            path expression
     * @return the {@link ConfigObject} value at the requested path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to an object
     */
    ConfigObject getObject(String path);

    /**
     * @param path
     *            path expression
     * @return the nested {@code Config} value at the requested path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a Config
     */
    Config getConfig(String path);

    /**
     * Gets the value at the path as an unwrapped Java boxed value (
     * {@link java.lang.Boolean Boolean}, {@link java.lang.Integer Integer}, and
     * so on - see {@link ConfigValue#unwrapped()}).
     *
     * @param path
     *            path expression
     * @return the unwrapped value at the requested path
     * @throws ConfigException.Missing
     *             if value is absent or null
     */
    Object getAnyRef(String path);

    /**
     * Gets the value at the given path, unless the value is a
     * null value or missing, in which case it throws just like
     * the other getters. Use {@code get()} on the {@link
     * Config#root()} object (or other object in the tree) if you
     * want an unprocessed value.
     *
     * @param path
     *            path expression
     * @return the value at the requested path
     * @throws ConfigException.Missing
     *             if value is absent or null
     */
    ConfigValue getValue(String path);

    /**
     * Gets a value as a size in bytes (parses special strings like "128M"). If
     * the value is already a number, then it's left alone; if it's a string,
     * it's parsed understanding unit suffixes such as "128K", as documented in
     * the <a
     * href="https://github.com/lightbend/config/blob/master/HOCON.md">the
     * spec</a>.
     *
     * @param path
     *            path expression
     * @return the value at the requested path, in bytes
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to Long or String
     * @throws ConfigException.BadValue
     *             if value cannot be parsed as a size in bytes
     */
    Long getBytes(String path);

    /**
     * Gets a value as an amount of memory (parses special strings like "128M"). If
     * the value is already a number, then it's left alone; if it's a string,
     * it's parsed understanding unit suffixes such as "128K", as documented in
     * the <a
     * href="https://github.com/lightbend/config/blob/master/HOCON.md">the
     * spec</a>.
     *
     * @since 1.3.0
     *
     * @param path
     *            path expression
     * @return the value at the requested path, in bytes
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to Long or String
     * @throws ConfigException.BadValue
     *             if value cannot be parsed as a size in bytes
     */
    ConfigMemorySize getMemorySize(String path);

    /**
     * Get value as a duration in milliseconds. If the value is already a
     * number, then it's left alone; if it's a string, it's parsed understanding
     * units suffixes like "10m" or "5ns" as documented in the <a
     * href="https://github.com/lightbend/config/blob/master/HOCON.md">the
     * spec</a>.
     *
     * @deprecated  As of release 1.1, replaced by {@link #getDuration(String, TimeUnit)}
     *
     * @param path
     *            path expression
     * @return the duration value at the requested path, in milliseconds
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to Long or String
     * @throws ConfigException.BadValue
     *             if value cannot be parsed as a number of milliseconds
     */
    @Deprecated
    Long getMilliseconds(String path);

    /**
     * Get value as a duration in nanoseconds. If the value is already a number
     * it's taken as milliseconds and converted to nanoseconds. If it's a
     * string, it's parsed understanding unit suffixes, as for
     * {@link #getDuration(String, TimeUnit)}.
     *
     * @deprecated  As of release 1.1, replaced by {@link #getDuration(String, TimeUnit)}
     *
     * @param path
     *            path expression
     * @return the duration value at the requested path, in nanoseconds
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to Long or String
     * @throws ConfigException.BadValue
     *             if value cannot be parsed as a number of nanoseconds
     */
    @Deprecated
    Long getNanoseconds(String path);

    /**
     * Gets a value as a duration in a specified
     * {@link java.util.concurrent.TimeUnit TimeUnit}. If the value is already a
     * number, then it's taken as milliseconds and then converted to the
     * requested TimeUnit; if it's a string, it's parsed understanding units
     * suffixes like "10m" or "5ns" as documented in the <a
     * href="https://github.com/lightbend/config/blob/master/HOCON.md">the
     * spec</a>.
     * 
     * @since 1.2.0
     * 
     * @param path
     *            path expression
     * @param unit
     *            convert the return value to this time unit
     * @return the duration value at the requested path, in the given TimeUnit
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to Long or String
     * @throws ConfigException.BadValue
     *             if value cannot be parsed as a number of the given TimeUnit
     */
    long getDuration(String path, TimeUnit unit);

    /**
     * Gets a value as a java.time.Duration. If the value is
     * already a number, then it's taken as milliseconds; if it's
     * a string, it's parsed understanding units suffixes like
     * "10m" or "5ns" as documented in the <a
     * href="https://github.com/lightbend/config/blob/master/HOCON.md">the
     * spec</a>. This method never returns null.
     *
     * @since 1.3.0
     *
     * @param path
     *            path expression
     * @return the duration value at the requested path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to Long or String
     * @throws ConfigException.BadValue
     *             if value cannot be parsed as a number of the given TimeUnit
     */
    Duration getDuration(String path);

    /**
     * Gets a value as a java.time.Period. If the value is
     * already a number, then it's taken as days; if it's
     * a string, it's parsed understanding units suffixes like
     * "10d" or "5w" as documented in the <a
     * href="https://github.com/lightbend/config/blob/master/HOCON.md">the
     * spec</a>. This method never returns null.
     *
     * @since 1.3.0
     *
     * @param path
     *            path expression
     * @return the period value at the requested path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to Long or String
     * @throws ConfigException.BadValue
     *             if value cannot be parsed as a number of the given TimeUnit
     */
    Period getPeriod(String path);

    /**
     * Gets a value as a java.time.temporal.TemporalAmount.
     * This method will first try get get the value as a java.time.Duration, and if unsuccessful,
     * then as a java.time.Period.
     * This means that values like "5m" will be parsed as 5 minutes rather than 5 months
     * @param path path expression
     * @return the temporal value at the requested path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to Long or String
     * @throws ConfigException.BadValue
     *             if value cannot be parsed as a TemporalAmount
     */
    TemporalAmount getTemporal(String path);

    /**
     * Gets a list value (with any element type) as a {@link ConfigList}, which
     * implements {@code java.util.List<ConfigValue>}. Throws if the path is
     * unset or null.
     *
     * @param path
     *            the path to the list value.
     * @return the {@link ConfigList} at the path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a ConfigList
     */
    ConfigList getList(String path);

    /**
     * Gets a list value with boolean elements.  Throws if the
     * path is unset or null or not a list or contains values not
     * convertible to boolean.
     *
     * @param path
     *            the path to the list value.
     * @return the list at the path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a list of booleans
     */
    List<Boolean> getBooleanList(String path);

    /**
     * Gets a list value with number elements.  Throws if the
     * path is unset or null or not a list or contains values not
     * convertible to number.
     *
     * @param path
     *            the path to the list value.
     * @return the list at the path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a list of numbers
     */
    List<Number> getNumberList(String path);

    /**
     * Gets a list value with int elements.  Throws if the
     * path is unset or null or not a list or contains values not
     * convertible to int.
     *
     * @param path
     *            the path to the list value.
     * @return the list at the path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a list of ints
     */
    List<Integer> getIntList(String path);

    /**
     * Gets a list value with long elements.  Throws if the
     * path is unset or null or not a list or contains values not
     * convertible to long.
     *
     * @param path
     *            the path to the list value.
     * @return the list at the path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a list of longs
     */
    List<Long> getLongList(String path);

    /**
     * Gets a list value with double elements.  Throws if the
     * path is unset or null or not a list or contains values not
     * convertible to double.
     *
     * @param path
     *            the path to the list value.
     * @return the list at the path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a list of doubles
     */
    List<Double> getDoubleList(String path);

    /**
     * Gets a list value with string elements.  Throws if the
     * path is unset or null or not a list or contains values not
     * convertible to string.
     *
     * @param path
     *            the path to the list value.
     * @return the list at the path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a list of strings
     */
    List<String> getStringList(String path);

    /**
     * Gets a list value with {@code Enum} elements.  Throws if the
     * path is unset or null or not a list or contains values not
     * convertible to {@code Enum}.
     *
     * @param enumClass
     *            the enum class
     * @param <T>
     *            a generic denoting a specific type of enum
     * @param path
     *            the path to the list value.
     * @return the list at the path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a list of {@code Enum}
     */
    <T extends Enum<T>> List<T> getEnumList(Class<T> enumClass, String path);

    /**
     * Gets a list value with object elements.  Throws if the
     * path is unset or null or not a list or contains values not
     * convertible to <code>ConfigObject</code>.
     *
     * @param path
     *            the path to the list value.
     * @return the list at the path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a list of objects
     */
    List<? extends ConfigObject> getObjectList(String path);

    /**
     * Gets a list value with <code>Config</code> elements.
     * Throws if the path is unset or null or not a list or
     * contains values not convertible to <code>Config</code>.
     *
     * @param path
     *            the path to the list value.
     * @return the list at the path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a list of configs
     */
    List<? extends Config> getConfigList(String path);

    /**
     * Gets a list value with any kind of elements.  Throws if the
     * path is unset or null or not a list. Each element is
     * "unwrapped" (see {@link ConfigValue#unwrapped()}).
     *
     * @param path
     *            the path to the list value.
     * @return the list at the path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a list
     */
    List<? extends Object> getAnyRefList(String path);

    /**
     * Gets a list value with elements representing a size in
     * bytes.  Throws if the path is unset or null or not a list
     * or contains values not convertible to memory sizes.
     *
     * @param path
     *            the path to the list value.
     * @return the list at the path
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a list of memory sizes
     */
    List<Long> getBytesList(String path);

    /**
     * Gets a list, converting each value in the list to a memory size, using the
     * same rules as {@link #getMemorySize(String)}.
     *
     * @since 1.3.0
     * @param path
     *            a path expression
     * @return list of memory sizes
     * @throws ConfigException.Missing
     *             if value is absent or null
     * @throws ConfigException.WrongType
     *             if value is not convertible to a list of memory sizes
     */
    List<ConfigMemorySize> getMemorySizeList(String path);

    /**
     * @deprecated  As of release 1.1, replaced by {@link #getDurationList(String, TimeUnit)}
     * @param path the path
     * @return list of millisecond values
     */
    @Deprecated
    List<Long> getMillisecondsList(String path);

    /**
     * @deprecated  As of release 1.1, replaced by {@link #getDurationList(String, TimeUnit)}
     * @param path the path
     * @return list of nanosecond values
     */
    @Deprecated
    List<Long> getNanosecondsList(String path);

    /**
     * Gets a list, converting each value in the list to a duration, using the
     * same rules as {@link #getDuration(String, TimeUnit)}.
     *
     * @since 1.2.0
     * @param path
     *            a path expression
     * @param unit
     *            time units of the returned values
     * @return list of durations, in the requested units
     */
    List<Long> getDurationList(String path, TimeUnit unit);

    /**
     * Gets a list, converting each value in the list to a duration, using the
     * same rules as {@link #getDuration(String)}.
     *
     * @since 1.3.0
     * @param path
     *            a path expression
     * @return list of durations
     */
    List<Duration> getDurationList(String path);

    /**
     * Clone the config with only the given path (and its children) retained;
     * all sibling paths are removed.
     * <p>
     * Note that path expressions have a syntax and sometimes require quoting
     * (see {@link ConfigUtil#joinPath} and {@link ConfigUtil#splitPath}).
     * 
     * @param path
     *            path to keep
     * @return a copy of the config minus all paths except the one specified
     */
    Config withOnlyPath(String path);

    /**
     * Clone the config with the given path removed.
     * <p>
     * Note that path expressions have a syntax and sometimes require quoting
     * (see {@link ConfigUtil#joinPath} and {@link ConfigUtil#splitPath}).
     * 
     * @param path
     *            path expression to remove
     * @return a copy of the config minus the specified path
     */
    Config withoutPath(String path);

    /**
     * Places the config inside another {@code Config} at the given path.
     * <p>
     * Note that path expressions have a syntax and sometimes require quoting
     * (see {@link ConfigUtil#joinPath} and {@link ConfigUtil#splitPath}).
     * 
     * @param path
     *            path expression to store this config at.
     * @return a {@code Config} instance containing this config at the given
     *         path.
     */
    Config atPath(String path);

    /**
     * Places the config inside a {@code Config} at the given key. See also
     * atPath(). Note that a key is NOT a path expression (see
     * {@link ConfigUtil#joinPath} and {@link ConfigUtil#splitPath}).
     * 
     * @param key
     *            key to store this config at.
     * @return a {@code Config} instance containing this config at the given
     *         key.
     */
    Config atKey(String key);

    /**
     * Returns a {@code Config} based on this one, but with the given path set
     * to the given value. Does not modify this instance (since it's immutable).
     * If the path already has a value, that value is replaced. To remove a
     * value, use withoutPath().
     * <p>
     * Note that path expressions have a syntax and sometimes require quoting
     * (see {@link ConfigUtil#joinPath} and {@link ConfigUtil#splitPath}).
     * 
     * @param path
     *            path expression for the value's new location
     * @param value
     *            value at the new path
     * @return the new instance with the new map entry
     */
    Config withValue(String path, ConfigValue value);
}
