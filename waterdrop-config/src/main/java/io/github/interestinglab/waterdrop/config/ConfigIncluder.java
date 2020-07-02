/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config;

/**
 * Implement this interface and provide an instance to
 * {@link ConfigParseOptions#setIncluder ConfigParseOptions.setIncluder()} to
 * customize handling of {@code include} statements in config files. You may
 * also want to implement {@link ConfigIncluderClasspath},
 * {@link ConfigIncluderFile}, and {@link ConfigIncluderURL}, or not.
 */
public interface ConfigIncluder {
    /**
     * Returns a new includer that falls back to the given includer. This is how
     * you can obtain the default includer; it will be provided as a fallback.
     * It's up to your includer to chain to it if you want to. You might want to
     * merge any files found by the fallback includer with any objects you load
     * yourself.
     *
     * It's important to handle the case where you already have the fallback
     * with a "return this", i.e. this method should not create a new object if
     * the fallback is the same one you already have. The same fallback may be
     * added repeatedly.
     *
     * @param fallback the previous includer for chaining
     * @return a new includer
     */
    ConfigIncluder withFallback(ConfigIncluder fallback);

    /**
     * Parses another item to be included. The returned object typically would
     * not have substitutions resolved. You can throw a ConfigException here to
     * abort parsing, or return an empty object, but may not return null.
     * 
     * This method is used for a "heuristic" include statement that does not
     * specify file, URL, or classpath resource. If the include statement does
     * specify, then the same class implementing {@link ConfigIncluder} must
     * also implement {@link ConfigIncluderClasspath},
     * {@link ConfigIncluderFile}, or {@link ConfigIncluderURL} as needed, or a
     * default includer will be used.
     * 
     * @param context
     *            some info about the include context
     * @param what
     *            the include statement's argument
     * @return a non-null ConfigObject
     */
    ConfigObject include(ConfigIncludeContext context, String what);
}
