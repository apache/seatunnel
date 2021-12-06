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
/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config;

import java.util.List;

/**
 * Subtype of {@link ConfigValue} representing a list value, as in JSON's
 * {@code [1,2,3]} syntax.
 *
 * <p>
 * {@code ConfigList} implements {@code java.util.List<ConfigValue>} so you can
 * use it like a regular Java list. Or call {@link #unwrapped()} to unwrap the
 * list elements into plain Java values.
 *
 * <p>
 * Like all {@link ConfigValue} subtypes, {@code ConfigList} is immutable. This
 * makes it threadsafe and you never have to create "defensive copies." The
 * mutator methods from {@link java.util.List} all throw
 * {@link java.lang.UnsupportedOperationException}.
 *
 * <p>
 * The {@link ConfigValue#valueType} method on a list returns
 * {@link ConfigValueType#LIST}.
 *
 * <p>
 * <em>Do not implement {@code ConfigList}</em>; it should only be implemented
 * by the config library. Arbitrary implementations will not work because the
 * library internals assume a specific concrete implementation. Also, this
 * interface is likely to grow new methods over time, so third-party
 * implementations will break.
 *
 */
public interface ConfigList extends List<ConfigValue>, ConfigValue {

    /**
     * Recursively unwraps the list, returning a list of plain Java values such
     * as Integer or String or whatever is in the list.
     *
     * @return a {@link java.util.List} containing plain Java objects
     */
    @Override
    List<Object> unwrapped();

    @Override
    ConfigList withOrigin(ConfigOrigin origin);
}
