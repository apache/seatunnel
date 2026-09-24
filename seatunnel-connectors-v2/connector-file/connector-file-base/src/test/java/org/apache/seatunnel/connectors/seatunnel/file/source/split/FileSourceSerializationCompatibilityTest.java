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

import org.apache.seatunnel.connectors.seatunnel.file.source.event.FileSplitFinishedEvent;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceOperationState;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;

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
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

class FileSourceSerializationCompatibilityTest {

    @TempDir private Path tempDir;

    @Test
    void testLegacyStatePreservesExistingFieldsAndDefaultsTailingState() throws Exception {
        FileSourceSplit split = new FileSourceSplit("table", "application.log");
        Map<Long, List<FileSourceOperationState>> pending =
                Collections.singletonMap(7L, Collections.emptyList());
        Map<String, Long> retention = Collections.singletonMap("backup", 42L);
        FileSourceState restored =
                restoreLegacyState(tempDir, Collections.singleton(split), 123L, pending, retention);

        Assertions.assertEquals(Collections.singleton(split), restored.getAssignedSplit());
        Assertions.assertEquals(123L, restored.getDiscoveryStartTimeMillis());
        Assertions.assertEquals(pending, restored.getPendingOpsByCheckpoint());
        Assertions.assertEquals(retention, restored.getRetentionLastRunMillisByPath());
        Assertions.assertTrue(restored.getProcessedFileOffsets().isEmpty());
        Assertions.assertTrue(restored.getFileTailStates().isEmpty());
        Assertions.assertTrue(restored.getInitialTailFileOffsets().isEmpty());
        Assertions.assertTrue(restored.getInitializedTailTables().isEmpty());
        Assertions.assertFalse(restored.isTextTailingInitialScanComplete());
    }

    @Test
    void testLegacyFinishedEventPreservesSplitAndFingerprint() throws Exception {
        FileSplitFinishedEvent restored = restoreLegacyEvent(tempDir, "split", "content");

        Assertions.assertEquals("split", restored.getSplitId());
        Assertions.assertEquals("content", restored.getContentFingerprint());
        // Pre-tailing events have no byte field; Java defaults it to zero, unlike new constructors.
        Assertions.assertEquals(0L, restored.getProcessedBytes());
    }

    static FileSourceState restoreLegacyState(
            Path tempDir,
            Set<FileSourceSplit> splits,
            long discoveryStart,
            Map<Long, List<FileSourceOperationState>> pending,
            Map<String, Long> retention)
            throws Exception {
        // This is the serialized layout before local text tailing was introduced.
        String source =
                "package org.apache.seatunnel.connectors.seatunnel.file.source.state;\n"
                        + "import java.io.Serializable;\n"
                        + "import java.util.Set;\n"
                        + "import java.util.Map;\n"
                        + "import java.util.List;\n"
                        + "import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;\n"
                        + "public class FileSourceState implements Serializable {\n"
                        + "  private static final long serialVersionUID = 9208369906513934611L;\n"
                        + "  private Set<FileSourceSplit> assignedSplit;\n"
                        + "  private long discoveryStartTimeMillis;\n"
                        + "  private Map<Long, List<FileSourceOperationState>> pendingOpsByCheckpoint;\n"
                        + "  private Map<String, Long> retentionLastRunMillisByPath;\n"
                        + "  public FileSourceState(Set<FileSourceSplit> splits, long start,\n"
                        + "      Map<Long, List<FileSourceOperationState>> pending, Map<String, Long> retention) {\n"
                        + "    assignedSplit = splits;\n"
                        + "    discoveryStartTimeMillis = start;\n"
                        + "    pendingOpsByCheckpoint = pending;\n"
                        + "    retentionLastRunMillisByPath = retention;\n"
                        + "  }\n"
                        + "}\n";
        return (FileSourceState)
                roundTripLegacy(
                        tempDir,
                        FileSourceState.class.getName(),
                        source,
                        new Class<?>[] {Set.class, long.class, Map.class, Map.class},
                        splits,
                        discoveryStart,
                        pending,
                        retention);
    }

    static FileSplitFinishedEvent restoreLegacyEvent(
            Path tempDir, String splitId, String fingerprint) throws Exception {
        String source =
                "package org.apache.seatunnel.connectors.seatunnel.file.source.event;\n"
                        + "import org.apache.seatunnel.api.source.SourceEvent;\n"
                        + "public class FileSplitFinishedEvent implements SourceEvent {\n"
                        + "  private static final long serialVersionUID = 1L;\n"
                        + "  private final String splitId;\n"
                        + "  private final String contentFingerprint;\n"
                        + "  public FileSplitFinishedEvent(String id, String fingerprint) {\n"
                        + "    splitId = id;\n"
                        + "    contentFingerprint = fingerprint;\n"
                        + "  }\n"
                        + "}\n";
        return (FileSplitFinishedEvent)
                roundTripLegacy(
                        tempDir,
                        FileSplitFinishedEvent.class.getName(),
                        source,
                        new Class<?>[] {String.class, String.class},
                        splitId,
                        fingerprint);
    }

    private static Object roundTripLegacy(
            Path tempDir,
            String className,
            String source,
            Class<?>[] parameters,
            Object... arguments)
            throws Exception {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        Assumptions.assumeTrue(
                compiler != null, "JDK compiler is required for legacy compatibility");
        Path sourceFile =
                tempDir.resolve("legacy-src").resolve(className.replace('.', '/') + ".java");
        Path outputRoot = tempDir.resolve("legacy-out");
        Files.createDirectories(sourceFile.getParent());
        Files.createDirectories(outputRoot);
        Files.write(sourceFile, source.getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals(
                0,
                compiler.run(
                        null,
                        null,
                        null,
                        "-classpath",
                        System.getProperty("java.class.path"),
                        "-d",
                        outputRoot.toString(),
                        sourceFile.toString()));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (URLClassLoader loader =
                new URLClassLoader(
                        new URL[] {outputRoot.toUri().toURL()},
                        FileSourceSerializationCompatibilityTest.class.getClassLoader()) {
                    @Override
                    protected Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        if (!className.equals(name)) {
                            return super.loadClass(name, resolve);
                        }
                        synchronized (getClassLoadingLock(name)) {
                            Class<?> loaded = findLoadedClass(name);
                            if (loaded == null) {
                                loaded = findClass(name);
                            }
                            if (resolve) {
                                resolveClass(loaded);
                            }
                            return loaded;
                        }
                    }
                }) {
            Class<?> legacyClass = Class.forName(className, true, loader);
            Assertions.assertSame(loader, legacyClass.getClassLoader());
            Object legacy = legacyClass.getConstructor(parameters).newInstance(arguments);
            try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
                output.writeObject(legacy);
            }
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return input.readObject();
        }
    }
}
