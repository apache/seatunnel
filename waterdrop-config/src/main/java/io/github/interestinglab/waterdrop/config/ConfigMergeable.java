/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config;

/**
 * Marker for types whose instances can be merged, that is {@link Config} and
 * {@link ConfigValue}. Instances of {@code Config} and {@code ConfigValue} can
 * be combined into a single new instance using the
 * {@link ConfigMergeable#withFallback withFallback()} method.
 *
 * <p>
 * <em>Do not implement this interface</em>; it should only be implemented by
 * the config library. Arbitrary implementations will not work because the
 * library internals assume a specific concrete implementation. Also, this
 * interface is likely to grow new methods over time, so third-party
 * implementations will break.
 */
public interface ConfigMergeable {
    /**
     * Returns a new value computed by merging this value with another, with
     * keys in this value "winning" over the other one.
     * 
     * <p>
     * This associative operation may be used to combine configurations from
     * multiple sources (such as multiple configuration files).
     * 
     * <p>
     * The semantics of merging are described in the <a
     * href="https://github.com/lightbend/config/blob/master/HOCON.md">spec
     * for HOCON</a>. Merging typically occurs when either the same object is
     * created twice in the same file, or two config files are both loaded. For
     * example:
     * 
     * <pre>
     *  foo = { a: 42 }
     *  foo = { b: 43 }
     * </pre>
     * 
     * Here, the two objects are merged as if you had written:
     * 
     * <pre>
     *  foo = { a: 42, b: 43 }
     * </pre>
     * 
     * <p>
     * Only {@link ConfigObject} and {@link Config} instances do anything in
     * this method (they need to merge the fallback keys into themselves). All
     * other values just return the original value, since they automatically
     * override any fallback. This means that objects do not merge "across"
     * non-objects; if you write
     * <code>object.withFallback(nonObject).withFallback(otherObject)</code>,
     * then <code>otherObject</code> will simply be ignored. This is an
     * intentional part of how merging works, because non-objects such as
     * strings and integers replace (rather than merging with) any prior value:
     * 
     * <pre>
     * foo = { a: 42 }
     * foo = 10
     * </pre>
     * 
     * Here, the number 10 "wins" and the value of <code>foo</code> would be
     * simply 10. Again, for details see the spec.
     * 
     * @param other
     *            an object whose keys should be used as fallbacks, if the keys
     *            are not present in this one
     * @return a new object (or the original one, if the fallback doesn't get
     *         used)
     */
    ConfigMergeable withFallback(ConfigMergeable other);
}
