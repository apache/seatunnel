/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.interestinglab.waterdrop.config;

import io.github.interestinglab.waterdrop.config.impl.ConfigImpl;

import java.net.URL;

/**
 * This class contains some static factory methods for building a {@link
 * ConfigOrigin}. {@code ConfigOrigin}s are automatically created when you
 * call other API methods to get a {@code ConfigValue} or {@code Config}.
 * But you can also set the origin of an existing {@code ConfigValue}, using
 * {@link ConfigValue#withOrigin(ConfigOrigin)}.
 *
 * @since 1.3.0
 */
public final class ConfigOriginFactory {
    private ConfigOriginFactory() {
    }

    /**
     * Returns the default origin for values when no other information is
     * provided. This is the origin used in {@link ConfigValueFactory
     * #fromAnyRef(Object)}.
     *
     * @since 1.3.0
     *
     * @return the default origin
     */
    public static ConfigOrigin newSimple() {
        return newSimple(null);
    }

    /**
     * Returns an origin with the given description.
     *
     *  @since 1.3.0
     *
     * @param description brief description of what the origin is
     * @return a new origin
     */
    public static ConfigOrigin newSimple(String description) {
        return ConfigImpl.newSimpleOrigin(description);
    }

    /**
     * Creates a file origin with the given filename.
     *
     * @since 1.3.0
     *
     * @param filename the filename of this origin
     * @return a new origin
     */
    public static ConfigOrigin newFile(String filename) {
        return ConfigImpl.newFileOrigin(filename);
    }

    /**
     * Creates a url origin with the given URL object.
     *
     * @since 1.3.0
     *
     * @param url the url of this origin
     * @return a new origin
     */
    public static ConfigOrigin newURL(URL url) {
        return ConfigImpl.newURLOrigin(url);
    }
}
