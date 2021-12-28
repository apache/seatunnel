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

package org.apache.seatunnel.config;

import org.apache.seatunnel.config.impl.ConfigImplUtil;

import java.util.List;

/**
 * Contains static utility methods.
 */
public final class ConfigUtil {
    private ConfigUtil() {

    }

    /**
     * Quotes and escapes a string, as in the JSON specification.
     *
     * @param s a string
     * @return the string quoted and escaped
     */
    public static String quoteString(String s) {
        return ConfigImplUtil.renderJsonString(s);
    }

    /**
     * Converts a list of keys to a path expression, by quoting the path
     * elements as needed and then joining them separated by a period. A path
     * expression is usable with a {@link Config}, while individual path
     * elements are usable with a {@link ConfigObject}.
     * <p>
     * See the overview documentation for {@link Config} for more detail on path
     * expressions vs. keys.
     *
     * @param elements the keys in the path
     * @return a path expression
     * @throws ConfigException if there are no elements
     */
    public static String joinPath(String... elements) {
        return ConfigImplUtil.joinPath(elements);
    }

    /**
     * Converts a list of strings to a path expression, by quoting the path
     * elements as needed and then joining them separated by a period. A path
     * expression is usable with a {@link Config}, while individual path
     * elements are usable with a {@link ConfigObject}.
     * <p>
     * See the overview documentation for {@link Config} for more detail on path
     * expressions vs. keys.
     *
     * @param elements the keys in the path
     * @return a path expression
     * @throws ConfigException if the list is empty
     */
    public static String joinPath(List<String> elements) {
        return ConfigImplUtil.joinPath(elements);
    }

    /**
     * Converts a path expression into a list of keys, by splitting on period
     * and unquoting the individual path elements. A path expression is usable
     * with a {@link Config}, while individual path elements are usable with a
     * {@link ConfigObject}.
     * <p>
     * See the overview documentation for {@link Config} for more detail on path
     * expressions vs. keys.
     *
     * @param path a path expression
     * @return the individual keys in the path
     * @throws ConfigException if the path expression is invalid
     */
    public static List<String> splitPath(String path) {
        return ConfigImplUtil.splitPath(path);
    }
}
