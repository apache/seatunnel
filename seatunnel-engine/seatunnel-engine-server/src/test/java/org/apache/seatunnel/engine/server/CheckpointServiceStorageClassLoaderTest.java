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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/** Verifies checkpoint factory discovery through the storage-specific Zeta classloader. */
public class CheckpointServiceStorageClassLoaderTest {

    private static final String TEST_STORAGE_TYPE = "isolated-test-storage";
    private static final String FACTORY_CLASS_NAME = "isolated.IsolatedCheckpointStorageFactory";

    @TempDir Path tempDir;

    /** Proves that the checkpoint constructor reads SPI resources from the split starter layout. */
    @Test
    public void testLoadsCheckpointFactoryFromZetaStarterJar() throws IOException {
        Path zetaDirectory = tempDir.resolve("zeta");
        createServiceJar(
                zetaDirectory.resolve("s3/checkpoint-test.jar"),
                CheckpointStorageFactory.class,
                FACTORY_CLASS_NAME,
                checkpointFactorySource());

        CheckpointStorageConfig storageConfig = new CheckpointStorageConfig();
        storageConfig.setStorage(TEST_STORAGE_TYPE);
        storageConfig.getStoragePluginConfig().put("storage.type", "s3");
        CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.setStorage(storageConfig);

        CheckpointService checkpointService =
                new CheckpointService(checkpointConfig, zetaDirectory);

        Assertions.assertNotNull(checkpointService.getCheckpointStorage());
    }

    private static void createServiceJar(
            Path jarPath, Class<?> serviceClass, String implementationClass, String source)
            throws IOException {
        Path compilationRoot = jarPath.getParent().resolve("compiled-provider");
        Path sourcePath =
                compilationRoot.resolve("src/" + implementationClass.replace('.', '/') + ".java");
        Path classesPath = compilationRoot.resolve("classes");
        Files.createDirectories(sourcePath.getParent());
        Files.createDirectories(classesPath);
        Files.write(sourcePath, source.getBytes(StandardCharsets.UTF_8));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        Assertions.assertNotNull(compiler, "A JDK compiler is required for this classloader test");
        String classPath =
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"));
        int compilationResult =
                compiler.run(
                        null,
                        null,
                        null,
                        "-proc:none",
                        "-classpath",
                        classPath,
                        "-d",
                        classesPath.toString(),
                        sourcePath.toString());
        Assertions.assertEquals(0, compilationResult, "The isolated test provider must compile");

        String classEntry = implementationClass.replace('.', '/') + ".class";
        Files.createDirectories(jarPath.getParent());
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jarPath))) {
            output.putNextEntry(new JarEntry(classEntry));
            Files.copy(classesPath.resolve(classEntry), output);
            output.closeEntry();
            output.putNextEntry(new JarEntry("META-INF/services/" + serviceClass.getName()));
            output.write(implementationClass.getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
    }

    /** Returns a factory implementation that is compiled only into the temporary starter jar. */
    private static String checkpointFactorySource() {
        return "package isolated;\n"
                + "public final class IsolatedCheckpointStorageFactory implements "
                + CheckpointStorageFactory.class.getName()
                + " {\n"
                + "  public String factoryIdentifier() { return \""
                + TEST_STORAGE_TYPE
                + "\"; }\n"
                + "  public "
                + CheckpointStorage.class.getName()
                + " create(java.util.Map<String, String> configuration) {\n"
                + "    return ("
                + CheckpointStorage.class.getName()
                + ") java.lang.reflect.Proxy.newProxyInstance(\n"
                + "        getClass().getClassLoader(), new Class<?>[] {"
                + CheckpointStorage.class.getName()
                + ".class}, (proxy, method, args) -> null);\n"
                + "  }\n"
                + "}\n";
    }
}
