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

import java.io.IOException;
import java.net.URI;

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
}
