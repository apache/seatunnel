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
package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileSourceSplitCompatibilityTest {

    private static final String LEGACY_SPLIT_CLASS_NAME =
            "org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit";

    @TempDir private Path tempDir;

    @Test
    void testDeserializeLegacyTwoArgSplitDefaultsToWholeFile() throws Exception {
        byte[] legacyBytes = serializeLegacySplit(tempDir, "t", "file:///tmp/test.txt");
        FileSourceSplit split = deserialize(legacyBytes);

        Assertions.assertEquals("t", split.getTableId());
        Assertions.assertEquals("file:///tmp/test.txt", split.getFilePath());
        Assertions.assertEquals(0L, split.getStart());
        Assertions.assertEquals(-1L, split.getLength());
        Assertions.assertEquals("t_file:///tmp/test.txt", split.splitId());
    }

    @Test
    void testDeserializeLegacySingleArgSplitDefaultsToWholeFile() throws Exception {
        byte[] legacyBytes = serializeLegacySplit(tempDir, "file:///tmp/test.txt");
        FileSourceSplit split = deserialize(legacyBytes);

        Assertions.assertNull(split.getTableId());
        Assertions.assertEquals("file:///tmp/test.txt", split.getFilePath());
        Assertions.assertEquals(0L, split.getStart());
        Assertions.assertEquals(-1L, split.getLength());
        Assertions.assertEquals("file:///tmp/test.txt", split.splitId());
    }

    private static FileSourceSplit deserialize(byte[] bytes) throws Exception {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            Object obj = in.readObject();
            Assertions.assertTrue(obj instanceof FileSourceSplit);
            return (FileSourceSplit) obj;
        }
    }

    private static byte[] serializeLegacySplit(Path tempDir, String tableId, String filePath)
            throws Exception {
        Class<?> legacyClass = compileAndLoadLegacyClass(tempDir);
        Constructor<?> ctor = legacyClass.getConstructor(String.class, String.class);
        Object legacySplit = ctor.newInstance(tableId, filePath);
        return serialize(legacySplit);
    }

    private static byte[] serializeLegacySplit(Path tempDir, String splitId) throws Exception {
        Class<?> legacyClass = compileAndLoadLegacyClass(tempDir);
        Constructor<?> ctor = legacyClass.getConstructor(String.class);
        Object legacySplit = ctor.newInstance(splitId);
        return serialize(legacySplit);
    }

    private static byte[] serialize(Object legacySplit) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(legacySplit);
        }
        return out.toByteArray();
    }

    private static Class<?> compileAndLoadLegacyClass(Path tempDir) throws Exception {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        Assumptions.assumeTrue(
                compiler != null, "JDK compiler is required for legacy compatibility test");

        Path sourceRoot = tempDir.resolve("legacy-src");
        Path outputRoot = tempDir.resolve("legacy-out");
        Path sourceFile =
                sourceRoot.resolve(
                        "org/apache/seatunnel/connectors/seatunnel/file/source/split/FileSourceSplit.java");
        Files.createDirectories(sourceFile.getParent());
        Files.createDirectories(outputRoot);

        Files.write(sourceFile, legacySourceCode().getBytes(StandardCharsets.UTF_8));

        String classpath = System.getProperty("java.class.path");
        int result =
                compiler.run(
                        null,
                        null,
                        null,
                        "-classpath",
                        classpath,
                        "-d",
                        outputRoot.toString(),
                        sourceFile.toString());
        Assertions.assertEquals(0, result, "Failed to compile legacy FileSourceSplit");

        URL[] urls = new URL[] {outputRoot.toUri().toURL()};
        try (ChildFirstClassLoader loader =
                new ChildFirstClassLoader(
                        urls, FileSourceSplitCompatibilityTest.class.getClassLoader())) {
            return Class.forName(LEGACY_SPLIT_CLASS_NAME, true, loader);
        }
    }

    private static String legacySourceCode() {
        return "package org.apache.seatunnel.connectors.seatunnel.file.source.split;\n"
                + "\n"
                + "import org.apache.seatunnel.api.source.SourceSplit;\n"
                + "\n"
                + "import java.util.Objects;\n"
                + "\n"
                + "public class FileSourceSplit implements SourceSplit {\n"
                + "    private static final long serialVersionUID = 1L;\n"
                + "\n"
                + "    private final String tableId;\n"
                + "    private final String filePath;\n"
                + "\n"
                + "    public FileSourceSplit(String splitId) {\n"
                + "        this.filePath = splitId;\n"
                + "        this.tableId = null;\n"
                + "    }\n"
                + "\n"
                + "    public FileSourceSplit(String tableId, String filePath) {\n"
                + "        this.tableId = tableId;\n"
                + "        this.filePath = filePath;\n"
                + "    }\n"
                + "\n"
                + "    @Override\n"
                + "    public String splitId() {\n"
                + "        if (tableId == null) {\n"
                + "            return filePath;\n"
                + "        }\n"
                + "        return tableId + \"_\" + filePath;\n"
                + "    }\n"
                + "\n"
                + "    @Override\n"
                + "    public boolean equals(Object o) {\n"
                + "        if (this == o) {\n"
                + "            return true;\n"
                + "        }\n"
                + "        if (o == null || getClass() != o.getClass()) {\n"
                + "            return false;\n"
                + "        }\n"
                + "        FileSourceSplit that = (FileSourceSplit) o;\n"
                + "        return Objects.equals(tableId, that.tableId)\n"
                + "                && Objects.equals(filePath, that.filePath);\n"
                + "    }\n"
                + "\n"
                + "    @Override\n"
                + "    public int hashCode() {\n"
                + "        return Objects.hash(tableId, filePath);\n"
                + "    }\n"
                + "}\n";
    }

    private static final class ChildFirstClassLoader extends URLClassLoader {
        private ChildFirstClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                if (LEGACY_SPLIT_CLASS_NAME.equals(name)) {
                    Class<?> loaded = findLoadedClass(name);
                    if (loaded == null) {
                        loaded = findClass(name);
                    }
                    if (resolve) {
                        resolveClass(loaded);
                    }
                    return loaded;
                }
                return super.loadClass(name, resolve);
            }
        }
    }
}
