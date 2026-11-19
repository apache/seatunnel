/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.aerospike;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestAerospikeFastjsonPackagingIT {
    @Test
    void connectorIncludesTheFastjsonDecimalFormatterLicense() throws Exception {
        Path artifact = Path.of(System.getProperty("aerospike.connectorJar"));
        try (JarFile jar = new JarFile(artifact.toFile())) {
            ZipEntry entry = jar.getJarEntry("META-INF/LICENSE");
            assertNotNull(entry);
            try (InputStream input = jar.getInputStream(entry)) {
                String license = new String(input.readAllBytes(), StandardCharsets.UTF_8);
                assertTrue(license.contains("com.alibaba.fastjson2.util.DoubleToDecimal"));
                assertTrue(
                        license.contains(
                                "Copyright (c) 2021, 2023, Oracle and/or its affiliates."));
                assertTrue(license.contains("Version 2, June 1991"));
                assertTrue(license.contains("\"CLASSPATH\" EXCEPTION TO THE GPL"));
                assertTrue(license.contains("fastjson2-2.0.33-sources.jar"));
            }
        }
    }

    @Test
    void connectorPreservesJsonbSymbolsWithoutTheOriginalMap() throws Exception {
        Path artifact = Path.of(System.getProperty("aerospike.connectorJar"));
        try (JarFile jar = new JarFile(artifact.toFile())) {
            String removedClass = "com/alibaba/fastjson2/internal/trove/map/hash/TLongIntHashMap";
            String replacementClass =
                    "org/apache/seatunnel/connectors/seatunnel/aerospike/fastjson/JsonbSymbolTable";
            assertFalse(jar.stream().anyMatch(entry -> entry.getName().startsWith(removedClass)));
            assertEquals(
                    List.of(replacementClass + ".class"),
                    jar.stream()
                            .map(ZipEntry::getName)
                            .filter(name -> name.startsWith(replacementClass))
                            .collect(Collectors.toList()));
        }

        // Load only the published form of the connector, independent of Maven's test classpath.
        try (URLClassLoader loader =
                new URLClassLoader(new URL[] {artifact.toUri().toURL()}, null)) {
            Class<?> jsonb = loader.loadClass("com.alibaba.fastjson2.JSONB");
            Method encode = jsonb.getMethod("toBytes", String.class);
            Class<?> writer = loader.loadClass("com.alibaba.fastjson2.JSONWriter");
            Class<?> feature = loader.loadClass("com.alibaba.fastjson2.JSONWriter$Feature");
            Object features = Array.newInstance(feature, 1);
            Array.set(features, 0, feature.getField("WriteNameAsSymbol").get(null));
            Method newWriter = writer.getMethod("ofJSONB", features.getClass());
            Method writeTypeName = writer.getMethod("writeTypeName", String.class);
            Method writeEncodedType = writer.getMethod("writeTypeName", byte[].class, long.class);
            Method writeName = writer.getMethod("writeNameRaw", byte[].class, long.class);
            Method getBytes = writer.getMethod("getBytes");

            // Golden bytes captured from the original shaded Fastjson2 2.0.33 connector.
            try (AutoCloseable jsonWriter = (AutoCloseable) newWriter.invoke(null, features)) {
                writeTypeName.invoke(jsonWriter, "typeA");
                writeTypeName.invoke(jsonWriter, "typeB");
                writeTypeName.invoke(jsonWriter, "typeA");
                Object typeC = encode.invoke(null, "typeC");
                writeEncodedType.invoke(jsonWriter, typeC, 29L);
                writeEncodedType.invoke(jsonWriter, typeC, 29L);
                Object name = encode.invoke(null, "name");
                writeName.invoke(jsonWriter, name, 0L);
                writeName.invoke(jsonWriter, name, 0L);
                writeName.invoke(jsonWriter, name, 42L);
                writeName.invoke(jsonWriter, name, 42L);
                assertArrayEquals(
                        Base64.getDecoder()
                                .decode(
                                        "kk50eXBlQQCSTnR5cGVCAZIAkk50eXBlQwKSAn9NbmFtZQN/TW5hbWUEf01uYW1lBX8F"),
                        (byte[]) getBytes.invoke(jsonWriter));
            }

            try (AutoCloseable jsonWriter = (AutoCloseable) newWriter.invoke(null, features)) {
                Object seed = encode.invoke(null, "seed");
                writeEncodedType.invoke(jsonWriter, seed, 1L);
                writeEncodedType.invoke(jsonWriter, seed, 1L);
                Object seen = encode.invoke(null, "seen");
                writeEncodedType.invoke(jsonWriter, seen, -1L);
                writeEncodedType.invoke(jsonWriter, seen, -1L);
                assertArrayEquals(
                        Base64.getDecoder().decode("kk1zZWVkAJIAkk1zZWVuAZIB"),
                        (byte[]) getBytes.invoke(jsonWriter));
            }
        }
    }
}
