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

import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

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
            org.apache.hadoop.fs.FileSystem fileSystem =
                    HadoopFileSystemProxy.wrapClasspathMismatch(proxy::getFileSystem);

            Assertions.assertNotNull(fileSystem);
        } finally {
            proxy.close();
        }
    }
}
