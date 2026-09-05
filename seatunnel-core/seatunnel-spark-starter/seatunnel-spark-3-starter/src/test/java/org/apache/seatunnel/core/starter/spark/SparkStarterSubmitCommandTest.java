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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.jar.JarFile;

public class SparkStarterSubmitCommandTest {

    @BeforeEach
    public void setUp() {
        SparkEngineTypeResolver.clearCache();
    }

    @AfterEach
    public void tearDown() {
        System.clearProperty(SparkEngineTypeResolver.SYSTEM_PROPERTY);
        SparkEngineTypeResolver.clearCache();
    }

    @Test
    public void testResolveSpark3FromSystemProperty() {
        System.setProperty(SparkEngineTypeResolver.SYSTEM_PROPERTY, EngineType.SPARK3.name());
        Assertions.assertEquals(EngineType.SPARK3, SparkEngineTypeResolver.resolve());
        Assertions.assertEquals(
                "seatunnel-spark-3-starter.jar",
                SparkEngineTypeResolver.resolve().getStarterJarName());
    }

    @Test
    public void testDefaultEngineTypeIsSpark3WhenUnset() {
        Assertions.assertEquals(EngineType.SPARK3, SparkEngineTypeResolver.resolve());
    }

    @Test
    @EnabledIf("starterJarExists")
    public void testShadedStarterJarManifestDeclaresSpark3() throws Exception {
        Path jarPath = Paths.get("target", "seatunnel-spark-3-starter.jar");
        try (JarFile jarFile = new JarFile(jarPath.toFile())) {
            String manifestEngineType =
                    jarFile.getManifest()
                            .getMainAttributes()
                            .getValue(SparkEngineTypeResolver.MANIFEST_ATTRIBUTE);
            Assertions.assertEquals(EngineType.SPARK3.name(), manifestEngineType);
        }
    }

    static boolean starterJarExists() {
        return Files.exists(Paths.get("target", "seatunnel-spark-3-starter.jar"));
    }
}
