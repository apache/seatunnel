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
 * Implement this interface and provide an instance to
 * {@link ConfigResolveOptions#appendResolver ConfigResolveOptions.appendResolver()}
 * to provide custom behavior when unresolved substitutions are encountered
 * during resolution.
 * @since 1.3.2
 */
public interface ConfigResolver {

    /**
     * Returns the value to substitute for the given unresolved path. To get the
     * components of the path use {@link ConfigUtil#splitPath(String)}. If a
     * non-null value is returned that value will be substituted, otherwise
     * resolution will continue to consider the substitution as still
     * unresolved.
     *
     * @param path the unresolved path
     * @return the value to use as a substitution or null
     */
    public ConfigValue lookup(String path);

    /**
     * Returns a new resolver that falls back to the given resolver if this
     * one doesn't provide a substitution itself.
     *
     * It's important to handle the case where you already have the fallback
     * with a "return this", i.e. this method should not create a new object if
     * the fallback is the same one you already have. The same fallback may be
     * added repeatedly.
     *
     * @param fallback the previous includer for chaining
     * @return a new resolver
     */
    public ConfigResolver withFallback(ConfigResolver fallback);

}
