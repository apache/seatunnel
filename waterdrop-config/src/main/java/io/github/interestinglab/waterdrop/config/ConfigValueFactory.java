/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config;

import io.github.interestinglab.waterdrop.config.impl.ConfigImpl;

import java.util.Map;

/**
 * This class holds some static factory methods for building {@link ConfigValue}
 * instances. See also {@link ConfigFactory} which has methods for parsing files
 * and certain in-memory data structures.
 */
public final class ConfigValueFactory {
    private ConfigValueFactory() {
    }

    /**
     * Creates a {@link ConfigValue} from a plain Java boxed value, which may be
     * a <code>Boolean</code>, <code>Number</code>, <code>String</code>,
     * <code>Map</code>, <code>Iterable</code>, or <code>null</code>. A
     * <code>Map</code> must be a <code>Map</code> from String to more values
     * that can be supplied to <code>fromAnyRef()</code>. An
     * <code>Iterable</code> must iterate over more values that can be supplied
     * to <code>fromAnyRef()</code>. A <code>Map</code> will become a
     * {@link ConfigObject} and an <code>Iterable</code> will become a
     * {@link ConfigList}. If the <code>Iterable</code> is not an ordered
     * collection, results could be strange, since <code>ConfigList</code> is
     * ordered.
     * 
     * <p>
     * In a <code>Map</code> passed to <code>fromAnyRef()</code>, the map's keys
     * are plain keys, not path expressions. So if your <code>Map</code> has a
     * key "foo.bar" then you will get one object with a key called "foo.bar",
     * rather than an object with a key "foo" containing another object with a
     * key "bar".
     * 
     * <p>
     * The originDescription will be used to set the origin() field on the
     * ConfigValue. It should normally be the name of the file the values came
     * from, or something short describing the value such as "default settings".
     * The originDescription is prefixed to error messages so users can tell
     * where problematic values are coming from.
     * 
     * <p>
     * Supplying the result of ConfigValue.unwrapped() to this function is
     * guaranteed to work and should give you back a ConfigValue that matches
     * the one you unwrapped. The re-wrapped ConfigValue will lose some
     * information that was present in the original such as its origin, but it
     * will have matching values.
     *
     * <p>
     * If you pass in a <code>ConfigValue</code> to this
     * function, it will be returned unmodified. (The
     * <code>originDescription</code> will be ignored in this
     * case.)
     *
     * <p>
     * This function throws if you supply a value that cannot be converted to a
     * ConfigValue, but supplying such a value is a bug in your program, so you
     * should never handle the exception. Just fix your program (or report a bug
     * against this library).
     * 
     * @param object
     *            object to convert to ConfigValue
     * @param originDescription
     *            name of origin file or brief description of what the value is
     * @return a new value
     */
    public static ConfigValue fromAnyRef(Object object, String originDescription) {
        return ConfigImpl.fromAnyRef(object, originDescription);
    }

    /**
     * See the {@link #fromAnyRef(Object, String)} documentation for details.
     * This is a typesafe wrapper that only works on {@link java.util.Map} and
     * returns {@link ConfigObject} rather than {@link ConfigValue}.
     *
     * <p>
     * If your <code>Map</code> has a key "foo.bar" then you will get one object
     * with a key called "foo.bar", rather than an object with a key "foo"
     * containing another object with a key "bar". The keys in the map are keys;
     * not path expressions. That is, the <code>Map</code> corresponds exactly
     * to a single {@code ConfigObject}. The keys will not be parsed or
     * modified, and the values are wrapped in ConfigValue. To get nested
     * {@code ConfigObject}, some of the values in the map would have to be more
     * maps.
     *
     * <p>
     * See also {@link ConfigFactory#parseMap(Map, String)} which interprets the
     * keys in the map as path expressions.
     *
     * @param values map from keys to plain Java values
     * @param originDescription description to use in {@link ConfigOrigin} of created values
     * @return a new {@link ConfigObject} value
     */
    public static ConfigObject fromMap(Map<String, ? extends Object> values,
                                       String originDescription) {
        return (ConfigObject) fromAnyRef(values, originDescription);
    }

    /**
     * See the {@link #fromAnyRef(Object, String)} documentation for details.
     * This is a typesafe wrapper that only works on {@link java.lang.Iterable}
     * and returns {@link ConfigList} rather than {@link ConfigValue}.
     *
     * @param values list of plain Java values
     * @param originDescription description to use in {@link ConfigOrigin} of created values
     * @return a new {@link ConfigList} value
     */
    public static ConfigList fromIterable(Iterable<? extends Object> values,
                                          String originDescription) {
        return (ConfigList) fromAnyRef(values, originDescription);
    }

    /**
     * See the other overload {@link #fromAnyRef(Object, String)} for details,
     * this one just uses a default origin description.
     *
     * @param object a plain Java value
     * @return a new {@link ConfigValue}
     */
    public static ConfigValue fromAnyRef(Object object) {
        return fromAnyRef(object, null);
    }

    /**
     * See the other overload {@link #fromMap(Map, String)} for details, this one
     * just uses a default origin description.
     *
     * <p>
     * See also {@link ConfigFactory#parseMap(Map)} which interprets the keys in
     * the map as path expressions.
     *
     * @param values map from keys to plain Java values
     * @return a new {@link ConfigObject}
     */
    public static ConfigObject fromMap(Map<String, ? extends Object> values) {
        return fromMap(values, null);
    }

    /**
     * See the other overload of {@link #fromIterable(Iterable, String)} for
     * details, this one just uses a default origin description.
     *
     * @param values list of plain Java values
     * @return a new {@link ConfigList}
     */
    public static ConfigList fromIterable(Iterable<? extends Object> values) {
        return fromIterable(values, null);
    }
}
