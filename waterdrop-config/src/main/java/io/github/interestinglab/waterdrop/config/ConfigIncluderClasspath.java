/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config;

/**
 * Implement this <em>in addition to</em> {@link ConfigIncluder} if you want to
 * support inclusion of files with the {@code include classpath("resource")}
 * syntax. If you do not implement this but do implement {@link ConfigIncluder},
 * attempts to load classpath resources will use the default includer.
 */
public interface ConfigIncluderClasspath {
    /**
     * Parses another item to be included. The returned object typically would
     * not have substitutions resolved. You can throw a ConfigException here to
     * abort parsing, or return an empty object, but may not return null.
     *
     * @param context
     *            some info about the include context
     * @param what
     *            the include statement's argument
     * @return a non-null ConfigObject
     */
    ConfigObject includeResources(ConfigIncludeContext context, String what);
}
