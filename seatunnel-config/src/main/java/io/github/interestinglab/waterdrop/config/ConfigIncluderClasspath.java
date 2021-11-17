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
