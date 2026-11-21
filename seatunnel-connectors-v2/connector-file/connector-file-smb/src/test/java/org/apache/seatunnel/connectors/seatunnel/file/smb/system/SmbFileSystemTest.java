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

package org.apache.seatunnel.connectors.seatunnel.file.smb.system;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import com.hierynomus.smbj.share.File;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SmbFileSystemTest {

    @Test
    void initializeShouldFailWithoutHost() {
        SmbFileSystem fs = new SmbFileSystem();
        Configuration conf = new Configuration();
        conf.set(SmbFileSystem.FS_SMB_USER, "user");
        conf.set(SmbFileSystem.FS_SMB_SHARE, "share");

        Assertions.assertThrows(
                IOException.class, () -> fs.initialize(URI.create("smb:///path"), conf));
    }

    @Test
    void initializeShouldFailWithoutUser() {
        SmbFileSystem fs = new SmbFileSystem();
        Configuration conf = new Configuration();
        conf.set(SmbFileSystem.FS_SMB_HOST, "myhost");
        conf.set(SmbFileSystem.FS_SMB_SHARE, "share");

        Assertions.assertThrows(
                IOException.class, () -> fs.initialize(URI.create("smb://myhost"), conf));
    }

    @Test
    void initializeShouldFailWithoutShare() {
        SmbFileSystem fs = new SmbFileSystem();
        Configuration conf = new Configuration();
        conf.set(SmbFileSystem.FS_SMB_HOST, "myhost");
        conf.set(SmbFileSystem.FS_SMB_USER, "user");

        Assertions.assertThrows(
                IOException.class, () -> fs.initialize(URI.create("smb://myhost"), conf));
    }

    @Test
    void schemeShouldBeSmb() {
        SmbFileSystem fs = new SmbFileSystem();
        Assertions.assertEquals("smb", fs.getScheme());
    }

    @Test
    void getWorkingDirectoryShouldReturnRoot() {
        SmbFileSystem fs = new SmbFileSystem();
        Assertions.assertEquals(new Path("/"), fs.getWorkingDirectory());
    }

    @Test
    void appendShouldThrowUnsupported() {
        SmbFileSystem fs = new SmbFileSystem();
        Assertions.assertThrows(
                IOException.class, () -> fs.append(new Path("/test.txt"), 1024, null));
    }

    @Test
    void initializeShouldUseDefaultPort() throws Exception {
        SmbFileSystem fs = new SmbFileSystem();
        Configuration conf = new Configuration();
        conf.set(SmbFileSystem.FS_SMB_HOST, "myhost");
        conf.set(SmbFileSystem.FS_SMB_USER, "user");
        conf.set(SmbFileSystem.FS_SMB_SHARE, "data");

        fs.initialize(URI.create("smb://myhost"), conf);
        Assertions.assertEquals(URI.create("smb://myhost"), fs.getUri());
    }

    @Test
    void initializeShouldUseCustomPort() throws Exception {
        SmbFileSystem fs = new SmbFileSystem();
        Configuration conf = new Configuration();
        conf.set(SmbFileSystem.FS_SMB_HOST, "myhost");
        conf.setInt(SmbFileSystem.FS_SMB_PORT, 4455);
        conf.set(SmbFileSystem.FS_SMB_USER, "user");
        conf.set(SmbFileSystem.FS_SMB_SHARE, "data");

        fs.initialize(URI.create("smb://myhost:4455"), conf);
        Assertions.assertEquals(URI.create("smb://myhost:4455"), fs.getUri());
    }

    @Test
    void toSmbPathShouldConvertSlashes() throws Exception {
        SmbFileSystem fs = initFs();
        try {
            String result = fs.toSmbPath(new Path("/data/subdir/file.txt"));
            Assertions.assertEquals("data\\subdir\\file.txt", result);
        } finally {
            fs.close();
        }
    }

    @Test
    void toSmbPathShouldHandleRootPath() throws Exception {
        SmbFileSystem fs = initFs();
        try {
            String result = fs.toSmbPath(new Path("/"));
            Assertions.assertEquals("", result);
        } finally {
            fs.close();
        }
    }

    @Test
    void smbConnectionCloseHandlesNulls() throws Exception {
        SmbConnection conn = new SmbConnection(null, null, null);
        Assertions.assertDoesNotThrow(conn::close);
    }

    // -- SmbInputStream seek + read tests --

    @Test
    void seekThenReadPassesOffsetToSmbFile() throws Exception {
        File mockFile = mock(File.class);
        SmbConnection mockConn = mock(SmbConnection.class);
        when(mockFile.read(any(byte[].class), eq(100L)))
                .thenAnswer(
                        inv -> {
                            byte[] buf = inv.getArgument(0);
                            Arrays.fill(buf, 0, Math.min(10, buf.length), (byte) 'A');
                            return Math.min(10, buf.length);
                        });

        SmbInputStream stream = new SmbInputStream(mockFile, mockConn, null);
        stream.seek(100);
        byte[] buf = new byte[10];
        int read = stream.read(buf, 0, 10);

        Assertions.assertEquals(10, read);
        verify(mockFile).read(any(byte[].class), eq(100L));
        Assertions.assertEquals(110, stream.getPos());
    }

    @Test
    void readAdvancesPosition() throws Exception {
        File mockFile = mock(File.class);
        SmbConnection mockConn = mock(SmbConnection.class);
        when(mockFile.read(any(byte[].class), eq(0L)))
                .thenAnswer(inv -> ((byte[]) inv.getArgument(0)).length);
        when(mockFile.read(any(byte[].class), eq(10L)))
                .thenAnswer(inv -> ((byte[]) inv.getArgument(0)).length);

        SmbInputStream stream = new SmbInputStream(mockFile, mockConn, null);
        stream.read(new byte[10], 0, 10);
        Assertions.assertEquals(10, stream.getPos());

        stream.read(new byte[5], 0, 5);
        Assertions.assertEquals(15, stream.getPos());
        verify(mockFile).read(any(byte[].class), eq(10L));
    }

    @Test
    void seekBackwardThenRead() throws Exception {
        File mockFile = mock(File.class);
        SmbConnection mockConn = mock(SmbConnection.class);
        when(mockFile.read(any(byte[].class), eq(0L)))
                .thenAnswer(inv -> ((byte[]) inv.getArgument(0)).length);
        when(mockFile.read(any(byte[].class), eq(10L)))
                .thenAnswer(inv -> ((byte[]) inv.getArgument(0)).length);

        SmbInputStream stream = new SmbInputStream(mockFile, mockConn, null);
        stream.read(new byte[10], 0, 10);
        Assertions.assertEquals(10, stream.getPos());

        stream.seek(0);
        Assertions.assertEquals(0, stream.getPos());

        stream.read(new byte[5], 0, 5);
        verify(mockFile, times(2)).read(any(byte[].class), eq(0L));
    }

    @Test
    void singleByteReadUsesPosition() throws Exception {
        File mockFile = mock(File.class);
        SmbConnection mockConn = mock(SmbConnection.class);
        when(mockFile.read(any(byte[].class), eq(50L)))
                .thenAnswer(
                        inv -> {
                            byte[] buf = inv.getArgument(0);
                            buf[0] = (byte) 'X';
                            return 1;
                        });

        SmbInputStream stream = new SmbInputStream(mockFile, mockConn, null);
        stream.seek(50);
        int b = stream.read();

        Assertions.assertEquals('X', b);
        Assertions.assertEquals(51, stream.getPos());
        verify(mockFile).read(any(byte[].class), eq(50L));
    }

    // -- SmbInputStream close lifecycle tests --

    @Test
    void closeStreamClosesBothFileAndConnection() throws Exception {
        File mockFile = mock(File.class);
        SmbConnection mockConn = mock(SmbConnection.class);

        SmbInputStream stream = new SmbInputStream(mockFile, mockConn, null);
        stream.close();

        InOrder order = inOrder(mockFile, mockConn);
        order.verify(mockFile).close();
        order.verify(mockConn).close();
    }

    @Test
    void closeStillClosesConnectionWhenFileCloseThrows() throws Exception {
        File mockFile = mock(File.class);
        SmbConnection mockConn = mock(SmbConnection.class);
        doThrow(new RuntimeException("file close failed")).when(mockFile).close();

        SmbInputStream stream = new SmbInputStream(mockFile, mockConn, null);
        IOException thrown = Assertions.assertThrows(IOException.class, stream::close);

        Assertions.assertTrue(thrown.getCause().getMessage().contains("file close failed"));
        verify(mockConn).close();
    }

    @Test
    void doubleCloseIsIdempotent() throws Exception {
        File mockFile = mock(File.class);
        SmbConnection mockConn = mock(SmbConnection.class);

        SmbInputStream stream = new SmbInputStream(mockFile, mockConn, null);
        stream.close();
        stream.close();

        verify(mockFile, times(1)).close();
        verify(mockConn, times(1)).close();
    }

    private SmbFileSystem initFs() throws Exception {
        SmbFileSystem fs = new SmbFileSystem();
        Configuration conf = new Configuration();
        conf.set(SmbFileSystem.FS_SMB_HOST, "myhost");
        conf.set(SmbFileSystem.FS_SMB_USER, "user");
        conf.set(SmbFileSystem.FS_SMB_SHARE, "data");
        fs.initialize(URI.create("smb://myhost"), conf);
        return fs;
    }
}
