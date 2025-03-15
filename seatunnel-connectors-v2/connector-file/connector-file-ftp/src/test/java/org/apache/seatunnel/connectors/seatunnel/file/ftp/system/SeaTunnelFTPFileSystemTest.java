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

package org.apache.seatunnel.connectors.seatunnel.file.ftp.system;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for SeaTunnelFTPFileSystem. */
public class SeaTunnelFTPFileSystemTest {

    private static final String USERNAME = "testuser";
    private static final String PASSWORD = "testpass";
    private static final String HOME_DIR = "/home/testuser";
    private static final int SERVER_PORT = 0; // Use random port

    private FakeFtpServer fakeFtpServer;
    private SeaTunnelFTPFileSystem ftpFileSystem;
    private Configuration conf;
    private int serverPort;

    @BeforeEach
    public void setUp() throws Exception {
        // Set up the mock FTP server
        fakeFtpServer = new FakeFtpServer();
        fakeFtpServer.setServerControlPort(SERVER_PORT);

        // Create user account
        UserAccount userAccount = new UserAccount(USERNAME, PASSWORD, HOME_DIR);
        fakeFtpServer.addUserAccount(userAccount);

        // Set up the file system
        FileSystem fileSystem = new UnixFakeFileSystem();
        fileSystem.add(new DirectoryEntry(HOME_DIR));
        fileSystem.add(new FileEntry(HOME_DIR + "/test.txt", "Test content"));
        fakeFtpServer.setFileSystem(fileSystem);

        // Start the FTP server
        fakeFtpServer.start();
        serverPort = fakeFtpServer.getServerControlPort();

        // Configure the FTP client
        conf = new Configuration();
        conf.set("fs.ftp.host", "localhost");
        conf.setInt("fs.ftp.host.port", serverPort);
        conf.set("fs.ftp.user.localhost", USERNAME);
        conf.set("fs.ftp.password.localhost", PASSWORD);

        // Initialize the FTP file system
        ftpFileSystem = new SeaTunnelFTPFileSystem();
        ftpFileSystem.initialize(new URI("ftp://localhost:" + serverPort), conf);
    }

    @AfterEach
    public void tearDown() {
        if (fakeFtpServer != null) {
            fakeFtpServer.stop();
        }
    }

    @Test
    public void testMkdirs() throws IOException {
        Path testDir = new Path(HOME_DIR + "/testDir/subDir");

        // Create parent directories recursively
        assertTrue(ftpFileSystem.mkdirs(testDir));

        // Verify both parent and child directories exist
        assertTrue(ftpFileSystem.exists(new Path(HOME_DIR + "/testDir")));
        assertTrue(ftpFileSystem.exists(testDir));

        // Verify it's really a directory
        FileStatus status = ftpFileSystem.getFileStatus(testDir);
        assertTrue(status.isDirectory());
    }

    @Test
    public void testCreateAndDeleteFile() throws IOException {
        Path testFile = new Path(HOME_DIR + "/newfile.txt");
        String content = "Hello, World!";

        // Create file
        try (FSDataOutputStream out =
                ftpFileSystem.create(testFile, null, false, 1024, (short) 1, 1024, null)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }

        // Verify file exists
        assertTrue(ftpFileSystem.exists(testFile));

        // Read file content
        try (FSDataInputStream in = ftpFileSystem.open(testFile, 1024)) {
            byte[] buffer = new byte[content.length()];
            in.readFully(buffer);
            assertEquals(content, new String(buffer, StandardCharsets.UTF_8));
        }

        // Delete file
        assertTrue(ftpFileSystem.delete(testFile, false));
        assertFalse(ftpFileSystem.exists(testFile));
    }

    @Test
    public void testListStatus() throws IOException {
        // Create test directory structure
        Path testDir = new Path(HOME_DIR + "/testListDir");
        ftpFileSystem.mkdirs(testDir, null);

        Path testFile1 = new Path(testDir, "file1.txt");
        Path testFile2 = new Path(testDir, "file2.txt");

        try (FSDataOutputStream out =
                ftpFileSystem.create(testFile1, null, false, 1024, (short) 1, 1024, null)) {
            out.write("content1".getBytes(StandardCharsets.UTF_8));
        }
        try (FSDataOutputStream out =
                ftpFileSystem.create(testFile2, null, false, 1024, (short) 1, 1024, null)) {
            out.write("content2".getBytes(StandardCharsets.UTF_8));
        }

        FileStatus[] statuses = ftpFileSystem.listStatus(testDir);
        assertEquals(2, statuses.length);

        // Clean up
        ftpFileSystem.delete(testDir, true);
    }

    @Test
    public void testRename() throws IOException {
        Path source = new Path(HOME_DIR + "/source.txt");
        Path target = new Path(HOME_DIR + "/target.txt");

        // Create source file
        try (FSDataOutputStream out =
                ftpFileSystem.create(source, null, false, 1024, (short) 1, 1024, null)) {
            out.write("test content".getBytes(StandardCharsets.UTF_8));
        }

        // Rename file
        assertTrue(ftpFileSystem.rename(source, target));
        assertFalse(ftpFileSystem.exists(source));
        assertTrue(ftpFileSystem.exists(target));
    }

    @Test
    public void testConnectionModes() throws Exception {
        // Test passive mode
        conf.set("fs.ftp.connection.mode", "PASSIVE_LOCAL");
        ftpFileSystem.initialize(new URI("ftp://localhost:" + serverPort), conf);
        Path testFile = new Path(HOME_DIR + "/passive_test.txt");
        assertTrue(ftpFileSystem.mkdirs(testFile.getParent(), null));

        // Test active mode
        conf.set("fs.ftp.connection.mode", "ACTIVE_LOCAL");
        ftpFileSystem.initialize(new URI("ftp://localhost:" + serverPort), conf);
        Path testFile2 = new Path(HOME_DIR + "/active_test.txt");
        assertTrue(ftpFileSystem.mkdirs(testFile2.getParent(), null));
    }

    @Test
    public void testMkdirsWithPermission() throws IOException {
        Path testDir = new Path(HOME_DIR + "/testDir/subDir");
        FsPermission permission = FsPermission.createImmutable((short) 0755); // rwxr-xr-x

        // Create parent directories recursively with permission
        assertTrue(ftpFileSystem.mkdirs(testDir, permission));

        // Verify both parent and child directories exist
        assertTrue(ftpFileSystem.exists(new Path(HOME_DIR + "/testDir")));
        assertTrue(ftpFileSystem.exists(testDir));

        // Verify it's really a directory
        FileStatus status = ftpFileSystem.getFileStatus(testDir);
        assertTrue(status.isDirectory());

        // Verify directory was created in the mock filesystem
        DirectoryEntry dirEntry =
                (DirectoryEntry) fakeFtpServer.getFileSystem().getEntry(testDir.toString());
        assertNotNull(dirEntry);
    }

    @Test
    public void testMkdirsWithNullPermission() throws IOException {
        Path testDir = new Path(HOME_DIR + "/testDir/subDir");

        // Create parent directories recursively with null permission
        assertTrue(ftpFileSystem.mkdirs(testDir, null));

        // Verify both parent and child directories exist
        assertTrue(ftpFileSystem.exists(new Path(HOME_DIR + "/testDir")));
        assertTrue(ftpFileSystem.exists(testDir));

        // Verify it's really a directory
        FileStatus status = ftpFileSystem.getFileStatus(testDir);
        assertTrue(status.isDirectory());
        // Don't verify the exact permission since it may vary by system
        assertNotNull(status.getPermission());
    }

    @Test
    public void testMkdirsWithNestedDirectories() throws IOException {
        Path deepDir = new Path(HOME_DIR + "/a/b/c/d");
        FsPermission permission = FsPermission.createImmutable((short) 0755);

        // Create nested directories
        assertTrue(ftpFileSystem.mkdirs(deepDir, permission));

        // Verify all parent directories exist
        assertTrue(ftpFileSystem.exists(new Path(HOME_DIR + "/a")));
        assertTrue(ftpFileSystem.exists(new Path(HOME_DIR + "/a/b")));
        assertTrue(ftpFileSystem.exists(new Path(HOME_DIR + "/a/b/c")));
        assertTrue(ftpFileSystem.exists(deepDir));

        // Verify all are directories
        assertTrue(ftpFileSystem.getFileStatus(deepDir).isDirectory());
    }

    @Test
    public void testMkdirsWithExistingDirectory() throws IOException {
        Path testDir = new Path(HOME_DIR + "/existing");

        // Create directory first time
        assertTrue(ftpFileSystem.mkdirs(testDir));

        // Try to create same directory again
        assertTrue(ftpFileSystem.mkdirs(testDir));

        // Verify it's still a directory
        assertTrue(ftpFileSystem.getFileStatus(testDir).isDirectory());
    }
}
