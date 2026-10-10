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

package org.apache.seatunnel.engine.core.classloader;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;

/**
 * Resolves serialized jar identities to the URLs loadable by one Engine instance.
 *
 * <p>The classloader service retains the original URLs as cache and reference-count identities and
 * invokes this dependency only when constructing a loader. Implementations must not mutate the
 * input collection and must preserve its ordering and jar correspondence. Resolution must remain
 * stable for the service lifetime. Shared implementations must be thread-safe; the service does not
 * own or close the resolver. Deployment-specific localization belongs in the caller's
 * implementation.
 */
@FunctionalInterface
public interface JarPathResolver {
    /**
     * Resolves the supplied jar URLs for loading on the local node.
     *
     * @param jars non-null collection of original serialized jar identities
     * @return non-null collection of corresponding loadable URLs
     * @throws IOException if a required local artifact cannot be resolved or validated
     * @throws URISyntaxException if a jar URL cannot be interpreted as the required URI
     */
    Collection<URL> resolve(Collection<URL> jars) throws IOException, URISyntaxException;

    /**
     * Creates a stateless resolver that preserves existing Engine jar-loading behavior.
     *
     * @return thread-safe resolver returning the supplied collection unchanged
     */
    static JarPathResolver identity() {
        return jars -> jars;
    }
}
