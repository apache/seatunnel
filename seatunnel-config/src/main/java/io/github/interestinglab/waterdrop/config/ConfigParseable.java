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

/**
 * An opaque handle to something that can be parsed, obtained from
 * {@link ConfigIncludeContext}.
 *
 * <p>
 * <em>Do not implement this interface</em>; it should only be implemented by
 * the config library. Arbitrary implementations will not work because the
 * library internals assume a specific concrete implementation. Also, this
 * interface is likely to grow new methods over time, so third-party
 * implementations will break.
 */
public interface ConfigParseable {
    /**
     * Parse whatever it is. The options should come from
     * {@link ConfigParseable#options options()} but you could tweak them if you
     * like.
     *
     * @param options parse options, should be based on the ones from
     *                {@link ConfigParseable#options options()}
     * @return the parsed object
     */
    ConfigObject parse(ConfigParseOptions options);

    /**
     * Returns a {@link ConfigOrigin} describing the origin of the parseable
     * item.
     *
     * @return the origin of the parseable item
     */
    ConfigOrigin origin();

    /**
     * Get the initial options, which can be modified then passed to parse().
     * These options will have the right description, includer, and other
     * parameters already set up.
     *
     * @return the initial options
     */
    ConfigParseOptions options();
}
