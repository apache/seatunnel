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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.connectors.seatunnel.file.hadoop.FileStatusListingSession;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class UpdateFileMetadataLoaderTest {

    @Test
    void shouldListDenseTargetDirectoryOnlyOnce() throws Exception {
        HadoopFileSystemProxy proxy = Mockito.mock(HadoopFileSystemProxy.class);
        AtomicInteger listCount = new AtomicInteger();
        FileStatusListingSession session =
                new FileStatusListingSession() {
                    @Override
                    public FileStatus getFileStatus(Path path) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void list(Path directory, FileStatusConsumer consumer)
                            throws java.io.IOException {
                        listCount.incrementAndGet();
                        for (int i = 0; i < 10_000; i++) {
                            consumer.accept(file(directory + "/file-" + i));
                        }
                    }

                    @Override
                    public void close() {}
                };
        Mockito.when(proxy.openFileStatusListingSession()).thenReturn(session);

        List<UpdateFileMetadataLoader.Request> requests = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            requests.add(new UpdateFileMetadataLoader.Request(i, "s3a://bucket/day/file-" + i));
        }

        UpdateFileMetadataLoader.Result result =
                UpdateFileMetadataLoader.load(requests, proxy, 8, 64);

        Assertions.assertEquals(1, listCount.get());
        Assertions.assertEquals(1, result.getBulkListedDirectories());
        Assertions.assertEquals(0, result.getPointLookups());
        Assertions.assertEquals(10_000, result.getStatuses().size());
        Mockito.verify(proxy, Mockito.never()).getFileStatus(Mockito.anyString());
    }

    @Test
    void shouldBoundSparsePointLookupsAndKeepRequestOrder() throws Exception {
        HadoopFileSystemProxy proxy = Mockito.mock(HadoopFileSystemProxy.class);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        Mockito.when(proxy.getFileStatus(Mockito.anyString()))
                .thenAnswer(
                        invocation -> {
                            int current = active.incrementAndGet();
                            peak.accumulateAndGet(current, Math::max);
                            try {
                                Thread.sleep(5);
                                return file(invocation.getArgument(0));
                            } finally {
                                active.decrementAndGet();
                            }
                        });

        List<UpdateFileMetadataLoader.Request> requests = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            requests.add(new UpdateFileMetadataLoader.Request(i, "s3a://bucket/day-" + i + "/f"));
        }

        UpdateFileMetadataLoader.Result result =
                UpdateFileMetadataLoader.load(requests, proxy, 4, 64);

        Assertions.assertEquals(100, result.getPointLookups());
        Assertions.assertTrue(peak.get() <= 4);
        Assertions.assertTrue(result.getPeakConcurrency() <= 4);
        Assertions.assertTrue(result.getPeakInFlight() <= 32);
        for (int i = 0; i < requests.size(); i++) {
            Assertions.assertEquals(
                    requests.get(i).getTargetPath(),
                    result.getStatuses().get(i).getPath().toString());
        }
    }

    @Test
    void shouldHonorDisabledBulkComparisonForFtpAndSftpTargets() throws Exception {
        HadoopFileSystemProxy proxy = Mockito.mock(HadoopFileSystemProxy.class);
        FileStatusListingSession session = Mockito.mock(FileStatusListingSession.class);
        Mockito.when(proxy.openFileStatusListingSession()).thenReturn(session);
        Mockito.when(proxy.getFileStatus(Mockito.anyString()))
                .thenAnswer(invocation -> file(invocation.getArgument(0)));
        List<UpdateFileMetadataLoader.Request> requests =
                Collections.singletonList(
                        new UpdateFileMetadataLoader.Request(0, "sftp://host/path/file.bin"));

        UpdateFileMetadataLoader.Result result =
                UpdateFileMetadataLoader.load(requests, proxy, 8, 0);

        Assertions.assertEquals(0, result.getBulkListedDirectories());
        Assertions.assertEquals(1, result.getPointLookups());
        Mockito.verify(proxy).getFileStatus("sftp://host/path/file.bin");
        Mockito.verify(session, Mockito.never()).list(Mockito.any(), Mockito.any());
    }

    @Test
    void shouldUsePointLookupsWhenAutomaticBulkComparisonIsDisabled() throws Exception {
        HadoopFileSystemProxy proxy = Mockito.mock(HadoopFileSystemProxy.class);
        Mockito.when(proxy.getFileStatus(Mockito.anyString()))
                .thenAnswer(invocation -> file(invocation.getArgument(0)));
        List<UpdateFileMetadataLoader.Request> requests = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            requests.add(new UpdateFileMetadataLoader.Request(i, "s3a://bucket/day/file-" + i));
        }

        UpdateFileMetadataLoader.Result result =
                UpdateFileMetadataLoader.load(requests, proxy, 4, 0);

        Assertions.assertEquals(0, result.getBulkListedDirectories());
        Assertions.assertEquals(100, result.getPointLookups());
        Mockito.verify(proxy, Mockito.never()).openFileStatusListingSession();
    }

    @Test
    void shouldUseNamedDaemonThreadsForPointLookups() throws Exception {
        HadoopFileSystemProxy proxy = Mockito.mock(HadoopFileSystemProxy.class);
        AtomicReference<Thread> lookupThread = new AtomicReference<>();
        Mockito.when(proxy.getFileStatus(Mockito.anyString()))
                .thenAnswer(
                        invocation -> {
                            lookupThread.set(Thread.currentThread());
                            return file(invocation.getArgument(0));
                        });

        UpdateFileMetadataLoader.load(
                Collections.singletonList(
                        new UpdateFileMetadataLoader.Request(0, "s3a://bucket/day/file")),
                proxy,
                1,
                0);

        Assertions.assertNotNull(lookupThread.get());
        Assertions.assertTrue(lookupThread.get().isDaemon());
        Assertions.assertTrue(
                lookupThread.get().getName().startsWith("seatunnel-file-update-lookup-"));
    }

    @Test
    void shouldUsePointLookupWhenRelativeTargetHasNoParent() throws Exception {
        HadoopFileSystemProxy proxy = Mockito.mock(HadoopFileSystemProxy.class);
        Mockito.when(proxy.getFileStatus("file.bin")).thenReturn(file("file.bin"));

        UpdateFileMetadataLoader.Result result =
                UpdateFileMetadataLoader.load(
                        Collections.singletonList(
                                new UpdateFileMetadataLoader.Request(0, "file.bin")),
                        proxy,
                        1,
                        1);

        Assertions.assertEquals(1, result.getPointLookups());
        Assertions.assertEquals(0, result.getBulkListedDirectories());
        Mockito.verify(proxy, Mockito.never()).openFileStatusListingSession();
    }

    @Test
    void shouldCancelPointLookupsAndReleaseWorkersAfterFirstFailure() throws Exception {
        HadoopFileSystemProxy proxy = Mockito.mock(HadoopFileSystemProxy.class);
        AtomicInteger active = new AtomicInteger();
        Mockito.when(proxy.getFileStatus(Mockito.anyString()))
                .thenAnswer(
                        invocation -> {
                            String path = invocation.getArgument(0);
                            if (path.endsWith("/fail")) {
                                throw new java.io.IOException("target unavailable");
                            }
                            active.incrementAndGet();
                            try {
                                Thread.sleep(30_000);
                                return file(path);
                            } finally {
                                active.decrementAndGet();
                            }
                        });
        List<UpdateFileMetadataLoader.Request> requests = new ArrayList<>();
        requests.add(new UpdateFileMetadataLoader.Request(0, "s3a://bucket/a/fail"));
        for (int i = 1; i < 20; i++) {
            requests.add(
                    new UpdateFileMetadataLoader.Request(i, "s3a://bucket/dir-" + i + "/file"));
        }

        Assertions.assertThrows(
                java.io.IOException.class,
                () -> UpdateFileMetadataLoader.load(requests, proxy, 4, 64));
        Assertions.assertEquals(0, active.get());
    }

    private static FileStatus file(String path) {
        return new FileStatus(1, false, 1, 1, 1, new Path(path));
    }
}
