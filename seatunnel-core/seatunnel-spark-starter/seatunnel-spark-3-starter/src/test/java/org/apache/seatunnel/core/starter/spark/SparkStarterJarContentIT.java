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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.jar.JarFile;
import java.util.stream.Stream;

public class SparkStarterJarContentIT {

    @Test
    public void testShadedJarDoesNotContainScalaClasses() throws IOException {
        try (Stream<Path> files = Files.list(Paths.get("target"))) {
            Path starterJar =
                    files.filter(path -> path.getFileName().toString().endsWith(".jar"))
                            .filter(path -> !path.getFileName().toString().startsWith("original-"))
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Spark starter jar is missing"));

            try (JarFile jarFile = new JarFile(starterJar.toFile())) {
                Assertions.assertFalse(
                        jarFile.stream().anyMatch(entry -> entry.getName().startsWith("scala/")),
                        "The Spark starter must use Spark's Scala runtime instead of embedding one");
            }
        }
    }
}
