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

package org.apache.seatunnel.connectors.seatunnel.file.sftp.system;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

import java.io.IOException;
import java.net.URI;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

class SftpFileSystemTest {

    @Test
    void mkdirsAcceptsDirectoryCreatedConcurrently() throws Exception {
        ChannelSftp client = mockClient();
        AtomicBoolean created = new AtomicBoolean();
        Mockito.when(client.ls("/"))
                .thenAnswer(ignored -> created.get() ? entries(true) : new Vector<>());
        Mockito.doAnswer(
                        ignored -> {
                            created.set(true);
                            throw new SftpException(ChannelSftp.SSH_FX_FAILURE, "Already exists");
                        })
                .when(client)
                .mkdir(Mockito.anyString());

        Assertions.assertTrue(mkdirs(client, new Path("/shared")));
        Mockito.verify(client, Mockito.never()).cd(Mockito.anyString());
    }

    @Test
    void mkdirsRejectsFileCreatedConcurrently() throws Exception {
        ChannelSftp client = mockClient();
        AtomicBoolean created = new AtomicBoolean();
        Mockito.when(client.ls("/"))
                .thenAnswer(ignored -> created.get() ? entries(false) : new Vector<>());
        SftpException failure = new SftpException(ChannelSftp.SSH_FX_FAILURE, "Already exists");
        Mockito.doAnswer(
                        ignored -> {
                            created.set(true);
                            throw failure;
                        })
                .when(client)
                .mkdir(Mockito.anyString());

        IOException error =
                Assertions.assertThrows(
                        IOException.class, () -> mkdirs(client, new Path("/shared")));
        Assertions.assertSame(failure, error.getCause());
    }

    @Test
    void mkdirsPreservesPermissionFailure() throws Exception {
        ChannelSftp client = mockClient();
        Mockito.when(client.ls("/")).thenReturn(new Vector<>());
        SftpException failure =
                new SftpException(ChannelSftp.SSH_FX_PERMISSION_DENIED, "Permission denied");
        Mockito.doThrow(failure).when(client).mkdir(Mockito.anyString());

        IOException error =
                Assertions.assertThrows(
                        IOException.class, () -> mkdirs(client, new Path("/shared")));
        Assertions.assertSame(failure, error.getCause());
    }

    @Test
    void mkdirsExistingDirectoryDoesNotWrite() throws Exception {
        ChannelSftp client = mockClient();
        Vector<ChannelSftp.LsEntry> directory = entries(true);
        Mockito.when(client.ls("/")).thenReturn(directory);

        Assertions.assertTrue(mkdirs(client, new Path("/shared")));
        Mockito.verify(client, Mockito.never()).mkdir(Mockito.anyString());
    }

    @Test
    void mkdirsExistingFileIsRejected() throws Exception {
        ChannelSftp client = mockClient();
        Vector<ChannelSftp.LsEntry> file = entries(false);
        Mockito.when(client.ls("/")).thenReturn(file);

        Assertions.assertThrows(IOException.class, () -> mkdirs(client, new Path("/shared")));
        Mockito.verify(client, Mockito.never()).mkdir(Mockito.anyString());
    }

    @Test
    void mkdirsResolvesRelativePathWithoutChangingChannelDirectory() throws Exception {
        ChannelSftp client = mockClient();
        Mockito.when(client.pwd()).thenReturn("/home");
        Vector<ChannelSftp.LsEntry> home = entries("home", true);
        Mockito.when(client.ls("/")).thenReturn(home);
        Mockito.when(client.ls("/home")).thenReturn(new Vector<>());

        Assertions.assertTrue(mkdirs(client, new Path("shared")));
        Mockito.verify(client).mkdir("/home/shared");
        Mockito.verify(client, Mockito.never()).cd(Mockito.anyString());
    }

    @Test
    void mkdirsCreatesChildAfterParentIsCreatedConcurrently() throws Exception {
        ChannelSftp client = mockClient();
        AtomicBoolean created = new AtomicBoolean();
        Mockito.when(client.ls("/"))
                .thenAnswer(ignored -> created.get() ? entries(true) : new Vector<>());
        Mockito.when(client.ls("/shared")).thenReturn(new Vector<>());
        Mockito.doAnswer(
                        ignored -> {
                            created.set(true);
                            throw new SftpException(ChannelSftp.SSH_FX_FAILURE, "Already exists");
                        })
                .when(client)
                .mkdir("/shared");

        Assertions.assertTrue(mkdirs(client, new Path("/shared/child")));
        Mockito.verify(client).mkdir("/shared/child");
        Mockito.verify(client, Mockito.never()).cd(Mockito.anyString());
    }

    private ChannelSftp mockClient() throws SftpException {
        ChannelSftp client = Mockito.mock(ChannelSftp.class);
        Mockito.when(client.pwd()).thenReturn("/");
        return client;
    }

    private Vector<ChannelSftp.LsEntry> entries(boolean directory) {
        return entries("shared", directory);
    }

    private Vector<ChannelSftp.LsEntry> entries(String name, boolean directory) {
        ChannelSftp.LsEntry entry = Mockito.mock(ChannelSftp.LsEntry.class);
        SftpATTRS attributes = Mockito.mock(SftpATTRS.class);
        Mockito.when(entry.getFilename()).thenReturn(name);
        Mockito.when(entry.getAttrs()).thenReturn(attributes);
        Mockito.when(attributes.isDir()).thenReturn(directory);
        Vector<ChannelSftp.LsEntry> entries = new Vector<>();
        entries.add(entry);
        return entries;
    }

    private boolean mkdirs(ChannelSftp client, Path path) throws Exception {
        try (MockedConstruction<SFTPConnectionPool> ignored =
                        Mockito.mockConstruction(
                                SFTPConnectionPool.class,
                                (pool, context) ->
                                        Mockito.when(pool.connect("host", 22, "user", null, null))
                                                .thenReturn(client));
                SFTPFileSystem fs = new SFTPFileSystem()) {
            Configuration conf = new Configuration(false);
            conf.set(SFTPFileSystem.FS_SFTP_USER_PREFIX + "host", "user");
            fs.initialize(URI.create("sftp://host:22"), conf);
            return fs.mkdirs(path);
        }
    }

    @Test
    void convertAllTypeFileName() {
        SFTPFileSystem sftpFileSystem = new SFTPFileSystem();
        Assertions.assertEquals(
                "/home/seatunnel/tmp/seatunnel/read/wildcard/e2e.txt",
                sftpFileSystem.quote("/home/seatunnel/tmp/seatunnel/read/wildcard/e2e.txt"));
        // test file name with wildcard '*'
        Assertions.assertEquals(
                "/home/seatunnel/tmp/seatunnel/read/wildcard/e\\*e.txt",
                sftpFileSystem.quote("/home/seatunnel/tmp/seatunnel/read/wildcard/e*e.txt"));

        // test file name with wildcard '?'
        Assertions.assertEquals(
                "/home/seatunnel/tmp/seatunnel/read/wildcard/e\\?e.txt",
                sftpFileSystem.quote("/home/seatunnel/tmp/seatunnel/read/wildcard/e?e.txt"));
    }
}
