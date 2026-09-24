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

package org.apache.seatunnel.resource.core.classloader;

import org.apache.seatunnel.engine.core.classloader.JarPathResolver;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

/**
 * Maps a localized application's distribution jars without changing their serialized identities.
 *
 * <p>Instances are immutable and may be shared across classloader requests. Their captured roots
 * define a stable mapping for the owning classloader service's lifetime; callers must preserve that
 * distribution layout while the service uses it. The resolver owns no resources requiring closure
 * and never mutates the input URL collection.
 */
public final class ApplicationJarPathResolver implements JarPathResolver {
    private final Path masterHome;
    private final Path localHome;

    /**
     * Captures immutable distribution roots for this worker's localized artifacts.
     *
     * @param masterHome absolute master distribution root
     * @param localHome absolute worker distribution root
     * @throws IllegalArgumentException if either root is relative
     * @throws NullPointerException if either root is null
     */
    public ApplicationJarPathResolver(String masterHome, String localHome) {
        this.masterHome = absoluteRoot(Objects.requireNonNull(masterHome, "masterHome"));
        this.localHome = absoluteRoot(Objects.requireNonNull(localHome, "localHome"));
    }

    private Path absoluteRoot(String value) {
        Path root = Paths.get(value);
        if (!root.isAbsolute()) {
            throw new IllegalArgumentException(
                    "Application distribution root must be absolute: " + value);
        }
        return root.normalize();
    }

    /**
     * Maps local file URLs anchored under the master distribution into this worker's distribution.
     *
     * <p>Sibling paths and non-local URLs retain their identities. Mapped paths must exist and
     * remain within the local root after normalization and symbolic-link resolution. The resolver
     * holds no mutable state and can be shared by concurrent callers.
     *
     * @param jars serialized jar URLs, which are never mutated
     * @return corresponding local URLs in input order
     * @throws IOException if a mapped jar is missing or escapes either distribution root
     * @throws URISyntaxException if a local file URL cannot be converted to a path
     */
    @Override
    public Collection<URL> resolve(Collection<URL> jars) throws IOException, URISyntaxException {
        Collection<URL> localized = new ArrayList<>(jars.size());
        for (URL jar : jars) {
            if (!"file".equals(jar.getProtocol())
                    || (jar.getAuthority() != null && !jar.getAuthority().isEmpty())) {
                localized.add(jar);
                continue;
            }
            Path original = Paths.get(jar.toURI());
            if (!original.startsWith(masterHome)) {
                localized.add(jar);
                continue;
            }
            Path normalized = original.normalize();
            if (!normalized.startsWith(masterHome)) {
                throw new IOException(
                        "Application jar path escapes the master distribution: " + jar);
            }
            Path localJar = localHome.resolve(masterHome.relativize(normalized)).normalize();
            if (!localJar.startsWith(localHome)
                    || !Files.isRegularFile(localJar)
                    || !localJar.toRealPath().startsWith(localHome.toRealPath())) {
                throw new IOException(
                        "Application jar is missing or escapes the localized distribution: "
                                + localJar);
            }
            localized.add(localJar.toUri().toURL());
        }
        return localized;
    }
}
