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

package org.apache.seatunnel.connectors.seatunnel.file.hadoop;

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

class HadoopFileSystemProxyTest {

    @TempDir private java.nio.file.Path tempDir;

    @Test
    void testMakeQualifiedPathUsesConfiguredFileSystemUri() throws Exception {
        HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"));
        try {
            Path qualifiedPath = new Path(proxy.makeQualifiedPath("/backup/post-sync"));

            Assertions.assertEquals("file", qualifiedPath.toUri().getScheme());
            Assertions.assertEquals("/backup/post-sync", qualifiedPath.toUri().getPath());
        } finally {
            proxy.close();
        }
    }

    @Test
    void testRenameRejectsMissingSourceAndTarget() throws Exception {
        HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"));
        java.nio.file.Path source = tempDir.resolve("missing-source.bin");
        java.nio.file.Path target = tempDir.resolve("missing-target.bin");
        try {
            IOException error =
                    Assertions.assertThrows(
                            IOException.class,
                            () -> proxy.renameFile(source.toString(), target.toString(), false));

            Assertions.assertTrue(error.getMessage().contains(source.getFileName().toString()));
            Assertions.assertTrue(error.getMessage().contains(target.getFileName().toString()));
        } finally {
            proxy.close();
        }
    }

    @Test
    void testHasAnyFileUsesLazyListingAndRecursiveFlag() throws Exception {
        Path warehouse = new Path("gs://test-bucket/warehouse");
        FileSystem fileSystem = Mockito.mock(FileSystem.class);
        @SuppressWarnings("unchecked")
        RemoteIterator<LocatedFileStatus> directFiles = Mockito.mock(RemoteIterator.class);
        @SuppressWarnings("unchecked")
        RemoteIterator<LocatedFileStatus> recursiveFiles = Mockito.mock(RemoteIterator.class);
        Mockito.when(fileSystem.exists(warehouse)).thenReturn(true);
        Mockito.when(fileSystem.listFiles(warehouse, false)).thenReturn(directFiles);
        Mockito.when(fileSystem.listFiles(warehouse, true)).thenReturn(recursiveFiles);
        Mockito.when(directFiles.hasNext()).thenReturn(false);
        Mockito.when(recursiveFiles.hasNext()).thenReturn(true);

        try (HadoopFileSystemProxy proxy = newProxy(fileSystem)) {
            Assertions.assertFalse(proxy.hasAnyFile(warehouse.toString(), false));
            Assertions.assertTrue(proxy.hasAnyFile(warehouse.toString(), true));
        }

        Mockito.verify(fileSystem).listFiles(warehouse, false);
        Mockito.verify(fileSystem).listFiles(warehouse, true);
        Mockito.verify(directFiles, Mockito.never()).next();
        Mockito.verify(recursiveFiles, Mockito.never()).next();
    }

    @Test
    void testHasAnyFileSkipsListingWhenPathDoesNotExist() throws Exception {
        Path missingPath = new Path("gs://test-bucket/missing");
        FileSystem fileSystem = Mockito.mock(FileSystem.class);
        Mockito.when(fileSystem.exists(missingPath)).thenReturn(false);

        try (HadoopFileSystemProxy proxy = newProxy(fileSystem)) {
            Assertions.assertFalse(proxy.hasAnyFile(missingPath.toString(), true));
        }

        Mockito.verify(fileSystem, Mockito.never())
                .listFiles(Mockito.any(Path.class), Mockito.anyBoolean());
    }

    @Test
    void testRenameTreatsExistingTargetAsCompletedRetry() throws Exception {
        HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"));
        java.nio.file.Path source = tempDir.resolve("missing-source.bin");
        java.nio.file.Path target = tempDir.resolve("existing-target.bin");
        Files.write(target, "target".getBytes(StandardCharsets.UTF_8));
        try {
            proxy.renameFile(source.toString(), target.toString(), false);

            Assertions.assertEquals(
                    "target", new String(Files.readAllBytes(target), StandardCharsets.UTF_8));
        } finally {
            proxy.close();
        }
    }

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason = "Hadoop local filesystem rename requires native Windows support")
    void testRenameMovesExistingSource() throws Exception {
        HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"));
        java.nio.file.Path source = tempDir.resolve("source.bin");
        java.nio.file.Path target = tempDir.resolve("nested/target.bin");
        Files.write(source, "source".getBytes(StandardCharsets.UTF_8));
        try {
            proxy.renameFile(source.toString(), target.toString(), false);

            Assertions.assertFalse(Files.exists(source));
            Assertions.assertEquals(
                    "source", new String(Files.readAllBytes(target), StandardCharsets.UTF_8));
        } finally {
            proxy.close();
        }
    }

    @Test
    void testCloseClearsFileSystemSoProxyCanBeReused() throws Exception {
        HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"));
        try {
            FileSystem first = proxy.getFileSystem();

            proxy.close();

            Assertions.assertNotSame(first, proxy.getFileSystem());
        } finally {
            proxy.close();
        }
    }

    private static HadoopFileSystemProxy newProxy(FileSystem fileSystem) {
        HadoopFileSystemProxy proxy =
                Mockito.mock(HadoopFileSystemProxy.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(fileSystem).when(proxy).getFileSystem();
        return proxy;
    }

    @Test
    void testWrapClasspathMismatchRewritesNoSuchMethodErrorAsDiagnosticIOException() {
        assertRewrittenAsDiagnosticIOException(
                new NoSuchMethodError("org.apache.hadoop.fs.FsTracer.get"));
    }

    @Test
    void testWrapClasspathMismatchRewritesNoClassDefFoundError() {
        assertRewrittenAsDiagnosticIOException(
                new NoClassDefFoundError("org/apache/hadoop/tracing/TraceUtils"));
    }

    @Test
    void testWrapClasspathMismatchRewritesNoSuchFieldError() {
        assertRewrittenAsDiagnosticIOException(
                new NoSuchFieldError("org.apache.hadoop.fs.FileSystem.statistics"));
    }

    @Test
    void testWrapClasspathMismatchRewritesIncompatibleClassChangeError() {
        assertRewrittenAsDiagnosticIOException(
                new IncompatibleClassChangeError("org.apache.hadoop.hdfs.DFSClient"));
    }

    @Test
    void testWrapClasspathMismatchDoesNotSwallowUnrelatedErrors() {
        OutOfMemoryError original = new OutOfMemoryError("Java heap space");

        OutOfMemoryError thrown =
                Assertions.assertThrows(
                        OutOfMemoryError.class,
                        () ->
                                HadoopFileSystemProxy.wrapClasspathMismatch(
                                        () -> {
                                            throw original;
                                        }));

        Assertions.assertSame(original, thrown);
    }

    @Test
    void testWrapClasspathMismatchPropagatesIOExceptionUnchanged() {
        IOException original = new IOException("Connection refused");

        IOException thrown =
                Assertions.assertThrows(
                        IOException.class,
                        () ->
                                HadoopFileSystemProxy.wrapClasspathMismatch(
                                        () -> {
                                            throw original;
                                        }));

        Assertions.assertSame(original, thrown);
        Assertions.assertNull(thrown.getCause());
    }

    @Test
    void testWrapClasspathMismatchNamesTheConcreteLinkageFailure() {
        UnsatisfiedLinkError original = new UnsatisfiedLinkError("no hadoop in java.library.path");

        IOException wrapped = rewrite(original);

        Assertions.assertSame(original, wrapped.getCause());
        Assertions.assertTrue(wrapped.getMessage().contains("UnsatisfiedLinkError"));
        Assertions.assertTrue(
                wrapped.getMessage().contains("no hadoop in java.library.path"),
                "the concrete linkage failure should be inlined for logs that only capture "
                        + "getMessage()");
    }

    @Test
    void testWrapClasspathMismatchHandlesLinkageErrorWithoutMessage() {
        NoClassDefFoundError original = new NoClassDefFoundError();

        IOException wrapped = rewrite(original);

        Assertions.assertSame(original, wrapped.getCause());
        Assertions.assertTrue(wrapped.getMessage().contains("NoClassDefFoundError"));
        Assertions.assertFalse(wrapped.getMessage().contains("null"));
    }

    private static IOException rewrite(LinkageError original) {
        return Assertions.assertThrows(
                IOException.class,
                () ->
                        HadoopFileSystemProxy.wrapClasspathMismatch(
                                () -> {
                                    throw original;
                                }));
    }

    private static void assertRewrittenAsDiagnosticIOException(LinkageError original) {
        IOException wrapped = rewrite(original);

        Assertions.assertSame(original, wrapped.getCause());
        Assertions.assertTrue(wrapped.getMessage().contains("Hadoop client"));
        Assertions.assertTrue(wrapped.getMessage().contains("version mismatch"));
        Assertions.assertTrue(
                wrapped.getMessage().contains(original.getClass().getSimpleName()),
                "the diagnostic should name the concrete linkage error");
    }

    @Test
    void testWrapClasspathMismatchPassesThroughOnSuccess() throws Exception {
        HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(new HadoopConf("file:///"));
        try {
            FileSystem fileSystem =
                    HadoopFileSystemProxy.wrapClasspathMismatch(proxy::getFileSystem);

            Assertions.assertNotNull(fileSystem);
        } finally {
            proxy.close();
        }
    }
}
