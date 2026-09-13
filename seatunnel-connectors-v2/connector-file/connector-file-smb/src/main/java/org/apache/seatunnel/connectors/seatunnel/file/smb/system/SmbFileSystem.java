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

import org.apache.seatunnel.connectors.seatunnel.file.hadoop.FileStatusListingSession;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.StreamingFileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SmbFileSystem extends FileSystem implements StreamingFileSystem {

    public static final String FS_SMB_HOST = "fs.smb.host";
    public static final String FS_SMB_PORT = "fs.smb.port";
    public static final String FS_SMB_USER = "fs.smb.user";
    public static final String FS_SMB_PASSWORD = "fs.smb.password";
    public static final String FS_SMB_DOMAIN = "fs.smb.domain";
    public static final String FS_SMB_SHARE = "fs.smb.share";

    private static final int DEFAULT_SMB_PORT = 445;
    private static final int DEFAULT_BLOCK_SIZE = 4 * 1024;

    private URI uri;
    private SMBClient client;
    private String host;
    private int port;
    private String user;
    private String password;
    private String domain;
    private String share;

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        this.uri = uri;

        this.host = conf.get(FS_SMB_HOST, uri.getHost());
        this.port =
                conf.getInt(FS_SMB_PORT, uri.getPort() == -1 ? DEFAULT_SMB_PORT : uri.getPort());
        this.user = conf.get(FS_SMB_USER);
        this.password = conf.get(FS_SMB_PASSWORD, "");
        this.domain = conf.get(FS_SMB_DOMAIN, "");
        this.share = conf.get(FS_SMB_SHARE, "");

        if (host == null || host.isEmpty()) {
            throw new IOException("SMB host is not specified");
        }
        if (user == null || user.isEmpty()) {
            throw new IOException("SMB user is not specified");
        }
        if (share == null || share.isEmpty()) {
            throw new IOException("SMB share is not specified");
        }

        SmbConfig smbConfig = SmbConfig.builder().build();
        this.client = new SMBClient(smbConfig);
        setConf(conf);
    }

    @Override
    public String getScheme() {
        return "smb";
    }

    @Override
    public URI getUri() {
        return uri;
    }

    private DiskShare connectShare() throws IOException {
        try {
            Connection connection = client.connect(host, port);
            AuthenticationContext ac =
                    new AuthenticationContext(user, password.toCharArray(), domain);
            Session session = connection.authenticate(ac);
            return (DiskShare) session.connectShare(share);
        } catch (Exception e) {
            throw new IOException("Failed to connect to SMB share: " + share, e);
        }
    }

    private String toSmbPath(Path path) {
        String pathStr = path.toUri().getPath();
        if (pathStr.startsWith("/")) {
            pathStr = pathStr.substring(1);
        }
        return pathStr.replace('/', '\\');
    }

    private Path toHadoopPath(String smbPath) {
        String unixPath = smbPath.replace('\\', '/');
        if (!unixPath.startsWith("/")) {
            unixPath = "/" + unixPath;
        }
        return new Path(unixPath);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        DiskShare diskShare = connectShare();
        try {
            String smbPath = toSmbPath(f);
            File smbFile =
                    diskShare.openFile(
                            smbPath,
                            EnumSet.of(AccessMask.GENERIC_READ),
                            null,
                            SMB2ShareAccess.ALL,
                            SMB2CreateDisposition.FILE_OPEN,
                            null);
            InputStream is = smbFile.getInputStream();
            return new FSDataInputStream(new SmbInputStream(is, smbFile, diskShare, statistics));
        } catch (Exception e) {
            closeDiskShare(diskShare);
            throw new IOException("Failed to open file: " + f, e);
        }
    }

    @Override
    public FSDataOutputStream create(
            Path f,
            FsPermission permission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException {
        DiskShare diskShare = connectShare();
        try {
            String smbPath = toSmbPath(f);

            Path parent = f.getParent();
            if (parent != null) {
                mkdirs(diskShare, parent);
            }

            if (!overwrite && fileExists(diskShare, smbPath)) {
                throw new IOException("File already exists: " + f);
            }

            File smbFile =
                    diskShare.openFile(
                            smbPath,
                            EnumSet.of(AccessMask.GENERIC_WRITE),
                            EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL),
                            SMB2ShareAccess.ALL,
                            overwrite
                                    ? SMB2CreateDisposition.FILE_OVERWRITE_IF
                                    : SMB2CreateDisposition.FILE_CREATE,
                            null);
            OutputStream os = smbFile.getOutputStream();
            return new FSDataOutputStream(os, statistics) {
                @Override
                public void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        smbFile.close();
                        closeDiskShare(diskShare);
                    }
                }
            };
        } catch (IOException e) {
            closeDiskShare(diskShare);
            throw e;
        } catch (Exception e) {
            closeDiskShare(diskShare);
            throw new IOException("Failed to create file: " + f, e);
        }
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
            throws IOException {
        throw new IOException("Append is not supported by SMB file system");
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        DiskShare diskShare = connectShare();
        try {
            String srcPath = toSmbPath(src);
            String dstPath = toSmbPath(dst);

            if (!fileExists(diskShare, srcPath)) {
                throw new IOException("Source path does not exist: " + src);
            }

            Path dstParent = dst.getParent();
            if (dstParent != null) {
                mkdirs(diskShare, dstParent);
            }

            FileAllInformation srcInfo = diskShare.getFileInformation(srcPath);
            boolean isDir =
                    (srcInfo.getBasicInformation().getFileAttributes()
                                    & FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue())
                            != 0;

            if (isDir) {
                try (com.hierynomus.smbj.share.Directory dir =
                        diskShare.openDirectory(
                                srcPath,
                                EnumSet.of(AccessMask.GENERIC_ALL, AccessMask.DELETE),
                                null,
                                SMB2ShareAccess.ALL,
                                SMB2CreateDisposition.FILE_OPEN,
                                null)) {
                    dir.rename(dstPath);
                }
            } else {
                try (File file =
                        diskShare.openFile(
                                srcPath,
                                EnumSet.of(AccessMask.GENERIC_ALL, AccessMask.DELETE),
                                null,
                                SMB2ShareAccess.ALL,
                                SMB2CreateDisposition.FILE_OPEN,
                                null)) {
                    file.rename(dstPath);
                }
            }
            return true;
        } catch (Exception e) {
            throw new IOException("Failed to rename " + src + " to " + dst, e);
        } finally {
            closeDiskShare(diskShare);
        }
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        DiskShare diskShare = connectShare();
        try {
            String smbPath = toSmbPath(f);
            if (!fileExists(diskShare, smbPath)) {
                return false;
            }
            return delete(diskShare, smbPath, recursive);
        } finally {
            closeDiskShare(diskShare);
        }
    }

    private boolean delete(DiskShare diskShare, String smbPath, boolean recursive)
            throws IOException {
        FileAllInformation info;
        try {
            info = diskShare.getFileInformation(smbPath);
        } catch (Exception e) {
            return false;
        }
        boolean isDir =
                (info.getBasicInformation().getFileAttributes()
                                & FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue())
                        != 0;
        if (isDir) {
            if (recursive) {
                List<FileIdBothDirectoryInformation> children = diskShare.list(smbPath);
                for (FileIdBothDirectoryInformation child : children) {
                    String name = child.getFileName();
                    if (".".equals(name) || "..".equals(name)) {
                        continue;
                    }
                    String childPath = smbPath.isEmpty() ? name : smbPath + "\\" + name;
                    delete(diskShare, childPath, true);
                }
            }
            diskShare.rmdir(smbPath, recursive);
        } else {
            diskShare.rm(smbPath);
        }
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        DiskShare diskShare = connectShare();
        try {
            String smbPath = toSmbPath(f);
            if (!fileExists(diskShare, smbPath)) {
                throw new FileNotFoundException("Path does not exist: " + f);
            }

            FileAllInformation info = diskShare.getFileInformation(smbPath);
            boolean isDir =
                    (info.getBasicInformation().getFileAttributes()
                                    & FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue())
                            != 0;

            if (!isDir) {
                return new FileStatus[] {toFileStatus(info, f)};
            }

            List<FileIdBothDirectoryInformation> children = diskShare.list(smbPath);
            List<FileStatus> result = new ArrayList<>();
            for (FileIdBothDirectoryInformation child : children) {
                String name = child.getFileName();
                if (".".equals(name) || "..".equals(name)) {
                    continue;
                }
                Path childPath = new Path(f, name);
                result.add(toFileStatus(child, childPath));
            }
            return result.toArray(new FileStatus[0]);
        } finally {
            closeDiskShare(diskShare);
        }
    }

    @Override
    public void setWorkingDirectory(Path newDir) {}

    @Override
    public Path getWorkingDirectory() {
        return new Path("/");
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        DiskShare diskShare = connectShare();
        try {
            mkdirs(diskShare, f);
            return true;
        } finally {
            closeDiskShare(diskShare);
        }
    }

    private void mkdirs(DiskShare diskShare, Path f) throws IOException {
        String smbPath = toSmbPath(f);
        if (smbPath.isEmpty() || fileExists(diskShare, smbPath)) {
            return;
        }
        Path parent = f.getParent();
        if (parent != null) {
            mkdirs(diskShare, parent);
        }
        try {
            diskShare.mkdir(smbPath);
        } catch (Exception e) {
            if (!fileExists(diskShare, smbPath)) {
                throw new IOException("Failed to create directory: " + f, e);
            }
        }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        DiskShare diskShare = connectShare();
        try {
            String smbPath = toSmbPath(f);
            if (smbPath.isEmpty()) {
                return new FileStatus(
                        0,
                        true,
                        1,
                        DEFAULT_BLOCK_SIZE,
                        0,
                        f.makeQualified(getUri(), getWorkingDirectory()));
            }
            if (!fileExists(diskShare, smbPath)) {
                throw new FileNotFoundException("Path does not exist: " + f);
            }
            FileAllInformation info = diskShare.getFileInformation(smbPath);
            return toFileStatus(info, f);
        } finally {
            closeDiskShare(diskShare);
        }
    }

    @Override
    public FileStatusListingSession openFileStatusListingSession() throws IOException {
        return new SmbListingSession(connectShare());
    }

    private final class SmbListingSession implements FileStatusListingSession {
        private final DiskShare diskShare;

        private SmbListingSession(DiskShare diskShare) {
            this.diskShare = diskShare;
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            String smbPath = toSmbPath(path);
            if (smbPath.isEmpty()) {
                return new FileStatus(
                        0,
                        true,
                        1,
                        DEFAULT_BLOCK_SIZE,
                        0,
                        path.makeQualified(getUri(), getWorkingDirectory()));
            }
            if (!fileExists(diskShare, smbPath)) {
                throw new FileNotFoundException("Path does not exist: " + path);
            }
            FileAllInformation info = diskShare.getFileInformation(smbPath);
            return toFileStatus(info, path);
        }

        @Override
        public void list(Path directory, FileStatusConsumer consumer) throws IOException {
            String smbPath = toSmbPath(directory);
            List<FileIdBothDirectoryInformation> children = diskShare.list(smbPath);
            for (FileIdBothDirectoryInformation child : children) {
                String name = child.getFileName();
                if (".".equals(name) || "..".equals(name)) {
                    continue;
                }
                Path childPath = new Path(directory, name);
                consumer.accept(toFileStatus(child, childPath));
            }
        }

        @Override
        public void close() throws IOException {
            closeDiskShare(diskShare);
        }
    }

    private boolean fileExists(DiskShare diskShare, String smbPath) {
        try {
            diskShare.getFileInformation(smbPath);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private FileStatus toFileStatus(FileAllInformation info, Path path) {
        long length = info.getStandardInformation().getEndOfFile();
        boolean isDir =
                (info.getBasicInformation().getFileAttributes()
                                & FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue())
                        != 0;
        long modTime =
                info.getBasicInformation().getLastWriteTime() != null
                        ? info.getBasicInformation().getLastWriteTime().toEpochMillis()
                        : 0;
        long accessTime =
                info.getBasicInformation().getLastAccessTime() != null
                        ? info.getBasicInformation().getLastAccessTime().toEpochMillis()
                        : 0;
        return new FileStatus(
                length,
                isDir,
                1,
                DEFAULT_BLOCK_SIZE,
                modTime,
                accessTime,
                FsPermission.getDirDefault(),
                user,
                "",
                path.makeQualified(getUri(), getWorkingDirectory()));
    }

    private FileStatus toFileStatus(FileIdBothDirectoryInformation info, Path path) {
        long length = info.getEndOfFile();
        Set<FileAttributes> attrs = new HashSet<>();
        long fileAttributes = info.getFileAttributes();
        if ((fileAttributes & FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue()) != 0) {
            attrs.add(FileAttributes.FILE_ATTRIBUTE_DIRECTORY);
        }
        boolean isDir = attrs.contains(FileAttributes.FILE_ATTRIBUTE_DIRECTORY);
        long modTime =
                info.getLastWriteTime() != null ? info.getLastWriteTime().toEpochMillis() : 0;
        long accessTime =
                info.getLastAccessTime() != null ? info.getLastAccessTime().toEpochMillis() : 0;
        return new FileStatus(
                length,
                isDir,
                1,
                DEFAULT_BLOCK_SIZE,
                modTime,
                accessTime,
                FsPermission.getDirDefault(),
                user,
                "",
                path.makeQualified(getUri(), getWorkingDirectory()));
    }

    private void closeDiskShare(DiskShare diskShare) {
        if (diskShare != null) {
            try {
                diskShare.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
