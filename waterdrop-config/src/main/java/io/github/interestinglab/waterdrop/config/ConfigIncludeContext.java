/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package io.github.interestinglab.waterdrop.config;


/**
 * Context provided to a {@link ConfigIncluder}; this interface is only useful
 * inside a {@code ConfigIncluder} implementation, and is not intended for apps
 * to implement.
 *
 * <p>
 * <em>Do not implement this interface</em>; it should only be implemented by
 * the config library. Arbitrary implementations will not work because the
 * library internals assume a specific concrete implementation. Also, this
 * interface is likely to grow new methods over time, so third-party
 * implementations will break.
 */
public interface ConfigIncludeContext {
    /**
     * Tries to find a name relative to whatever is doing the including, for
     * example in the same directory as the file doing the including. Returns
     * null if it can't meaningfully create a relative name. The returned
     * parseable may not exist; this function is not required to do any IO, just
     * compute what the name would be.
     *
     * The passed-in filename has to be a complete name (with extension), not
     * just a basename. (Include statements in config files are allowed to give
     * just a basename.)
     *
     * @param filename
     *            the name to make relative to the resource doing the including
     * @return parseable item relative to the resource doing the including, or
     *         null
     */
    ConfigParseable relativeTo(String filename);

    /**
     * Parse options to use (if you use another method to get a
     * {@link ConfigParseable} then use {@link ConfigParseable#options()}
     * instead though).
     *
     * @return the parse options
     */
    ConfigParseOptions parseOptions();


    /**
     * Copy this {@link ConfigIncludeContext} giving it a new value for its parseOptions.
     *
     * @param options new parse options to use
     *
     * @return the updated copy of this context
     */
    ConfigIncludeContext setParseOptions(ConfigParseOptions options);
}
