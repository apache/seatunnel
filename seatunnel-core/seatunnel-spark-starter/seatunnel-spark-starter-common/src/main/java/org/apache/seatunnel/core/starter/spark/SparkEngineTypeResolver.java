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

package org.apache.seatunnel.core.starter.spark;

import org.apache.seatunnel.common.constants.EngineType;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.CodeSource;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Resolves the Spark engine type for the current starter jar.
 *
 * <p>Resolution order:
 *
 * <ol>
 *   <li>System property {@link #SYSTEM_PROPERTY}
 *   <li>Manifest entry {@link #MANIFEST_ATTRIBUTE} in the starter jar
 *   <li>Default {@link EngineType#SPARK3}
 * </ol>
 */
public final class SparkEngineTypeResolver {

    public static final String MANIFEST_ATTRIBUTE = "SeaTunnel-Spark-Engine-Type";

    public static final String SYSTEM_PROPERTY = "seatunnel.spark.engine";

    private static volatile EngineType cachedEngineType;

    private SparkEngineTypeResolver() {}

    /** Resolves the Spark engine type for command generation in the current starter runtime. */
    public static EngineType resolve() {
        EngineType resolved = cachedEngineType;
        if (resolved != null) {
            return resolved;
        }
        resolved = resolveEngineType();
        cachedEngineType = resolved;
        return resolved;
    }

    /** Clears the cached engine type. Intended for unit tests only. */
    static void clearCache() {
        cachedEngineType = null;
    }

    private static EngineType resolveEngineType() {
        String configuredEngineType = System.getProperty(SYSTEM_PROPERTY);
        if (configuredEngineType != null && !configuredEngineType.isEmpty()) {
            return EngineType.valueOf(configuredEngineType);
        }
        String manifestEngineType = readManifestEngineType();
        if (manifestEngineType != null && !manifestEngineType.isEmpty()) {
            return EngineType.valueOf(manifestEngineType);
        }
        return EngineType.SPARK3;
    }

    private static String readManifestEngineType() {
        CodeSource codeSource = SparkStarter.class.getProtectionDomain().getCodeSource();
        if (codeSource == null || codeSource.getLocation() == null) {
            return null;
        }
        try {
            File jarFile = new File(codeSource.getLocation().toURI());
            if (!jarFile.isFile()) {
                return null;
            }
            try (JarFile jar = new JarFile(jarFile)) {
                Manifest manifest = jar.getManifest();
                if (manifest == null) {
                    return null;
                }
                Attributes attributes = manifest.getMainAttributes();
                return attributes.getValue(MANIFEST_ATTRIBUTE);
            }
        } catch (URISyntaxException | IOException e) {
            return null;
        }
    }
}
