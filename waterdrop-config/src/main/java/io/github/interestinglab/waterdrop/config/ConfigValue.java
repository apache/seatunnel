/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config;

/**
 * An immutable value, following the <a href="http://json.org">JSON</a> type
 * schema.
 * 
 * <p>
 * Because this object is immutable, it is safe to use from multiple threads and
 * there's no need for "defensive copies."
 * 
 * <p>
 * <em>Do not implement interface {@code ConfigValue}</em>; it should only be
 * implemented by the config library. Arbitrary implementations will not work
 * because the library internals assume a specific concrete implementation.
 * Also, this interface is likely to grow new methods over time, so third-party
 * implementations will break.
 */
public interface ConfigValue extends ConfigMergeable {
    /**
     * The origin of the value (file, line number, etc.), for debugging and
     * error messages.
     *
     * @return where the value came from
     */
    ConfigOrigin origin();

    /**
     * The {@link ConfigValueType} of the value; matches the JSON type schema.
     *
     * @return value's type
     */
    ConfigValueType valueType();

    /**
     * Returns the value as a plain Java boxed value, that is, a {@code String},
     * {@code Number}, {@code Boolean}, {@code Map<String,Object>},
     * {@code List<Object>}, or {@code null}, matching the {@link #valueType()}
     * of this {@code ConfigValue}. If the value is a {@link ConfigObject} or
     * {@link ConfigList}, it is recursively unwrapped.
     * @return a plain Java value corresponding to this ConfigValue
     */
    Object unwrapped();

    /**
     * Renders the config value as a HOCON string. This method is primarily
     * intended for debugging, so it tries to add helpful comments and
     * whitespace.
     * 
     * <p>
     * If the config value has not been resolved (see {@link Config#resolve}),
     * it's possible that it can't be rendered as valid HOCON. In that case the
     * rendering should still be useful for debugging but you might not be able
     * to parse it. If the value has been resolved, it will always be parseable.
     * 
     * <p>
     * This method is equivalent to
     * {@code render(ConfigRenderOptions.defaults())}.
     * 
     * @return the rendered value
     */
    String render();

    /**
     * Renders the config value to a string, using the provided options.
     * 
     * <p>
     * If the config value has not been resolved (see {@link Config#resolve}),
     * it's possible that it can't be rendered as valid HOCON. In that case the
     * rendering should still be useful for debugging but you might not be able
     * to parse it. If the value has been resolved, it will always be parseable.
     * 
     * <p>
     * If the config value has been resolved and the options disable all
     * HOCON-specific features (such as comments), the rendering will be valid
     * JSON. If you enable HOCON-only features such as comments, the rendering
     * will not be valid JSON.
     * 
     * @param options
     *            the rendering options
     * @return the rendered value
     */
    String render(ConfigRenderOptions options);

    @Override
    ConfigValue withFallback(ConfigMergeable other);

    /**
     * Places the value inside a {@link Config} at the given path. See also
     * {@link ConfigValue#atKey(String)}.
     * 
     * @param path
     *            path to store this value at.
     * @return a {@code Config} instance containing this value at the given
     *         path.
     */
    Config atPath(String path);

    /**
     * Places the value inside a {@link Config} at the given key. See also
     * {@link ConfigValue#atPath(String)}.
     * 
     * @param key
     *            key to store this value at.
     * @return a {@code Config} instance containing this value at the given key.
     */
    Config atKey(String key);

    /**
     * Returns a {@code ConfigValue} based on this one, but with the given
     * origin. This is useful when you are parsing a new format of file or setting
     * comments for a single ConfigValue.
     *
     * @since 1.3.0
     *
     * @param origin the origin set on the returned value
     * @return the new ConfigValue with the given origin
     */
    ConfigValue withOrigin(ConfigOrigin origin);
}
