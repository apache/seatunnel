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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HadoopFileSystemProxy implements Serializable, Closeable {

    private transient UserGroupInformation userGroupInformation;
    private transient FileSystem fileSystem;

    private transient Configuration configuration;
    private final HadoopConf hadoopConf;
    private boolean isAuthTypeKerberos;

    public HadoopFileSystemProxy(@NonNull HadoopConf hadoopConf) {
        this.hadoopConf = hadoopConf;
        // eager initialization
        initialize();
    }

    public boolean fileExist(@NonNull String filePath) throws IOException {
        return execute(() -> getFileSystem().exists(new Path(filePath)));
    }

    /**
     * Qualifies a path against this proxy's configured filesystem.
     *
     * <p>This preserves the filesystem scheme and authority for paths configured without a URI,
     * such as a relative FTP backup path.
     *
     * @param filePath path to qualify
     * @return path qualified with the configured filesystem URI
     */
    public String makeQualifiedPath(@NonNull String filePath) {
        return getFileSystem().makeQualified(new Path(filePath)).toString();
    }

    public boolean isFile(@NonNull String filePath) throws IOException {
        return execute(() -> getFileSystem().getFileStatus(new Path(filePath)).isFile());
    }

    public void createFile(@NonNull String filePath) throws IOException {
        execute(
                () -> {
                    if (!getFileSystem().createNewFile(new Path(filePath))) {
                        throw CommonError.fileOperationFailed("SeaTunnel", "create", filePath);
                    }
                    return Void.class;
                });
    }

    public void deleteFile(@NonNull String filePath) throws IOException {
        execute(
                () -> {
                    Path path = new Path(filePath);
                    if (getFileSystem().exists(path)) {
                        if (!getFileSystem().delete(path, true)) {
                            throw CommonError.fileOperationFailed("SeaTunnel", "delete", filePath);
                        }
                    }
                    return Void.class;
                });
    }

    public boolean deleteEmptyDirectory(@NonNull String filePath) throws IOException {
        return execute(
                () -> {
                    Path path = new Path(filePath);
                    FileSystem fileSystem = getFileSystem();
                    try {
                        if (!fileSystem.exists(path)
                                || !fileSystem.getFileStatus(path).isDirectory()) {
                            return false;
                        }
                        FileStatus[] children = fileSystem.listStatus(path);
                        if (children != null && children.length > 0) {
                            return false;
                        }
                        return fileSystem.delete(path, false);
                    } catch (IOException e) {
                        log.debug(
                                "Skip deleting empty directory {}, it may be changed concurrently",
                                filePath,
                                e);
                        return false;
                    }
                });
    }

    public void renameFile(
            @NonNull String oldFilePath,
            @NonNull String newFilePath,
            boolean removeWhenNewFilePathExist)
            throws IOException {
        execute(
                () -> {
                    Path oldPath = new Path(oldFilePath);
                    Path newPath = new Path(newFilePath);

                    if (!fileExist(oldPath.toString())) {
                        if (fileExist(newPath.toString())) {
                            log.info(
                                    "Rename file from [{}] to [{}] already finished in a previous "
                                            + "commit, skip.",
                                    oldPath,
                                    newPath);
                            return Void.class;
                        }
                        throw new IOException(
                                "Cannot rename file from ["
                                        + oldPath
                                        + "] to ["
                                        + newPath
                                        + "]: both source and target are missing.");
                    }

                    if (removeWhenNewFilePathExist) {
                        if (fileExist(newFilePath)) {
                            getFileSystem().delete(newPath, true);
                            log.info("Delete already file: {}", newPath);
                        }
                    }
                    if (!fileExist(newPath.getParent().toString())) {
                        createDir(newPath.getParent().toString());
                    }

                    if (getFileSystem().rename(oldPath, newPath)) {
                        log.info("rename file :[" + oldPath + "] to [" + newPath + "] finish");
                    } else {
                        throw CommonError.fileOperationFailed(
                                "SeaTunnel", "rename", oldFilePath + " -> " + newFilePath);
                    }
                    return Void.class;
                });
    }

    public void createDir(@NonNull String filePath) throws IOException {
        execute(
                () -> {
                    Path dfs = new Path(filePath);
                    FileSystem fs = getFileSystem();

                    if (fs.mkdirs(dfs)) {
                        return Void.class;
                    }

                    if (fs.exists(dfs)) {
                        return Void.class;
                    }

                    IOException enhanced = enhanceMkdirsException(fs, dfs, "create directory");
                    throw CommonError.fileOperationFailed(
                            "SeaTunnel", "create", filePath, enhanced);
                });
    }

    public List<LocatedFileStatus> listFile(String path) throws IOException {
        return execute(
                () -> {
                    List<LocatedFileStatus> fileList = new ArrayList<>();
                    if (!fileExist(path)) {
                        return fileList;
                    }
                    Path fileName = new Path(path);
                    RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator =
                            getFileSystem().listFiles(fileName, false);
                    while (locatedFileStatusRemoteIterator.hasNext()) {
                        fileList.add(locatedFileStatusRemoteIterator.next());
                    }
                    return fileList;
                });
    }

    public List<Path> getAllSubFiles(@NonNull String filePath) throws IOException {
        return execute(
                () -> {
                    List<Path> pathList = new ArrayList<>();
                    if (!fileExist(filePath)) {
                        return pathList;
                    }
                    Path fileName = new Path(filePath);
                    FileStatus[] status = getFileSystem().listStatus(fileName);
                    if (status != null) {
                        for (FileStatus fileStatus : status) {
                            if (fileStatus.isDirectory()) {
                                pathList.add(fileStatus.getPath());
                            }
                        }
                    }
                    return pathList;
                });
    }

    public FileStatus[] listStatus(String filePath) throws IOException {
        return execute(() -> getFileSystem().listStatus(new Path(filePath)));
    }

    /** Opens one reusable directory-listing session for a complete discovery pass. */
    public FileStatusListingSession openFileStatusListingSession() throws IOException {
        FileSystem fs = getFileSystem();
        if (fs instanceof StreamingFileSystem) {
            return ((StreamingFileSystem) fs).openFileStatusListingSession();
        }
        return new HadoopListingSession();
    }

    public FileStatus getFileStatus(String filePath) throws IOException {
        return execute(() -> getFileSystem().getFileStatus(new Path(filePath)));
    }

    private final class HadoopListingSession implements FileStatusListingSession {
        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            return execute(() -> getFileSystem().getFileStatus(path));
        }

        @Override
        public void list(Path directory, FileStatusConsumer consumer) throws IOException {
            execute(
                    () -> {
                        FileSystem fs = getFileSystem();
                        int emitted = 0;
                        try {
                            RemoteIterator<? extends FileStatus> iterator;
                            if ("s3a".equalsIgnoreCase(fs.getScheme())) {
                                iterator = fs.listLocatedStatus(directory);
                            } else {
                                iterator = fs.listStatusIterator(directory);
                            }
                            while (iterator.hasNext()) {
                                consumer.accept(iterator.next());
                                emitted++;
                            }
                        } catch (UnsupportedOperationException e) {
                            if (emitted > 0) {
                                throw e;
                            }
                            for (FileStatus status : fs.listStatus(directory)) {
                                consumer.accept(status);
                            }
                        }
                        return Void.class;
                    });
        }

        @Override
        public void close() {
            // The proxy owns the Hadoop FileSystem lifecycle.
        }
    }

    public FileChecksum getFileChecksum(String filePath) throws IOException {
        return execute(() -> getFileSystem().getFileChecksum(new Path(filePath)));
    }

    public FSDataOutputStream getOutputStream(String filePath) throws IOException {
        return execute(
                () -> {
                    Path path = new Path(filePath);
                    FileSystem fs = getFileSystem();
                    try {
                        return fs.create(path, true);
                    } catch (IOException e) {
                        IOException enhanced =
                                enhanceMkdirsException(
                                        fs, path.getParent(), "create file " + path.getName(), e);
                        throw CommonError.fileOperationFailed(
                                "SeaTunnel", "create", filePath, enhanced);
                    }
                });
    }

    public FSDataInputStream getInputStream(String filePath) throws IOException {
        return execute(() -> getFileSystem().open(new Path(filePath)));
    }

    public FileSystem getFileSystem() {
        if (fileSystem == null) {
            initialize();
        }
        return fileSystem;
    }

    /** Returns the scheme of the initialized target or source file system. */
    public String getScheme() {
        return getFileSystem().getScheme();
    }

    @SneakyThrows
    public <T> T doWithHadoopAuth(HadoopLoginFactory.LoginFunction<T> loginFunction) {
        if (configuration == null) {
            this.configuration = createConfiguration();
        }
        if (enableKerberos()) {
            configuration.set("hadoop.security.authentication", "kerberos");
            return HadoopLoginFactory.loginWithKerberos(
                    configuration,
                    hadoopConf.getKrb5Path(),
                    hadoopConf.getKerberosPrincipal(),
                    hadoopConf.getKerberosKeytabPath(),
                    loginFunction);
        }
        if (enableRemoteUser()) {
            return HadoopLoginFactory.loginWithRemoteUser(
                    configuration, hadoopConf.getRemoteUser(), loginFunction);
        }
        return loginFunction.run(configuration, UserGroupInformation.getCurrentUser());
    }

    @Override
    public void close() throws IOException {
        try {
            if (userGroupInformation != null && isAuthTypeKerberos) {
                userGroupInformation.logoutUserFromKeytab();
            }
        } finally {
            if (fileSystem != null) {
                fileSystem.close();
            }
        }
    }

    @SneakyThrows
    private void initialize() {
        this.configuration = createConfiguration();
        if (enableKerberos()) {
            configuration.set("hadoop.security.authentication", "kerberos");
            initializeWithKerberosLogin();
            isAuthTypeKerberos = true;
            return;
        }
        if (enableRemoteUser()) {
            initializeWithRemoteUserLogin();
            isAuthTypeKerberos = true;
            return;
        }
        fileSystem = FileSystem.get(configuration);
        fileSystem.setWriteChecksum(false);
        isAuthTypeKerberos = false;
    }

    private Configuration createConfiguration() {
        Configuration configuration = hadoopConf.toConfiguration();
        hadoopConf.setExtraOptionsForConfiguration(configuration);
        return configuration;
    }

    private IOException enhanceMkdirsException(FileSystem fs, Path path, String operation)
            throws IOException {
        return enhanceMkdirsException(fs, path, operation, null);
    }

    private IOException enhanceMkdirsException(
            FileSystem fs, Path path, String operation, IOException cause) throws IOException {
        StringBuilder reason = new StringBuilder();

        if (!fs.exists(path)) {
            Path parent = path.getParent();
            if (parent != null && !fs.exists(parent)) {
                reason.append("Parent directory does not exist: ").append(parent).append(". ");
            } else {
                reason.append("Directory does not exist and creation failed: ")
                        .append(path)
                        .append(". ");
            }

            try {
                fs.getFileStatus(path);
            } catch (IOException e) {
                if (e.getMessage() != null) {
                    if (e.getMessage().contains("Permission denied")) {
                        reason.append("Permission denied. ");
                    } else {
                        reason.append("Hadoop error: ").append(e.getMessage()).append(". ");
                    }
                }
            }
        } else {
            reason.append("Path exists but may be inaccessible: ").append(path).append(". ");
        }

        reason.append("Operation: ")
                .append(operation)
                .append(". ")
                .append("Current working directory: ")
                .append(fs.getWorkingDirectory());

        IOException enhanced = new IOException(reason.toString());
        if (cause != null) {
            enhanced.addSuppressed(cause);
        }
        return enhanced;
    }

    private boolean enableKerberos() {
        boolean kerberosPrincipalEmpty = StringUtils.isBlank(hadoopConf.getKerberosPrincipal());
        boolean kerberosKeytabPathEmpty = StringUtils.isBlank(hadoopConf.getKerberosKeytabPath());
        if (kerberosKeytabPathEmpty && kerberosPrincipalEmpty) {
            return false;
        }
        if (!kerberosPrincipalEmpty && !kerberosKeytabPathEmpty) {
            return true;
        }
        if (kerberosPrincipalEmpty) {
            throw new IllegalArgumentException("Please set kerberosPrincipal");
        }
        throw new IllegalArgumentException("Please set kerberosKeytabPath");
    }

    private void initializeWithKerberosLogin() throws IOException, InterruptedException {
        Pair<UserGroupInformation, FileSystem> pair =
                HadoopLoginFactory.loginWithKerberos(
                        configuration,
                        hadoopConf.getKrb5Path(),
                        hadoopConf.getKerberosPrincipal(),
                        hadoopConf.getKerberosKeytabPath(),
                        (configuration, userGroupInformation) -> {
                            this.userGroupInformation = userGroupInformation;
                            this.fileSystem = FileSystem.get(configuration);
                            return Pair.of(userGroupInformation, fileSystem);
                        });
        userGroupInformation = pair.getKey();
        fileSystem = pair.getValue();
        fileSystem.setWriteChecksum(false);
        log.info("Create FileSystem success with Kerberos: {}.", hadoopConf.getKerberosPrincipal());
    }

    private boolean enableRemoteUser() {
        return StringUtils.isNotBlank(hadoopConf.getRemoteUser());
    }

    private void initializeWithRemoteUserLogin() throws Exception {
        final Pair<UserGroupInformation, FileSystem> pair =
                HadoopLoginFactory.loginWithRemoteUser(
                        configuration,
                        hadoopConf.getRemoteUser(),
                        (configuration, userGroupInformation) -> {
                            this.userGroupInformation = userGroupInformation;
                            this.fileSystem = FileSystem.get(configuration);
                            return Pair.of(userGroupInformation, fileSystem);
                        });
        log.info("Create FileSystem success with RemoteUser: {}.", hadoopConf.getRemoteUser());
        userGroupInformation = pair.getKey();
        fileSystem = pair.getValue();
        fileSystem.setWriteChecksum(false);
    }

    private <T> T execute(PrivilegedExceptionAction<T> action) throws IOException {
        // The execute method is used to handle privileged actions, ensuring that the correct
        // user context (Kerberos or otherwise) is applied when performing file system operations.
        // This is necessary to maintain security and proper access control in a Hadoop environment.
        // If kerberos is disabled, the action is run directly. If kerberos is enabled, the action
        // is run as a privileged action using the doAsPrivileged method.
        if (isAuthTypeKerberos) {
            return doAsPrivileged(action);
        } else {
            try {
                return action.run();
            } catch (IOException | SeaTunnelRuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private <T> T doAsPrivileged(PrivilegedExceptionAction<T> action) throws IOException {
        if (fileSystem == null || userGroupInformation == null) {
            initialize();
        }

        try {
            // Ensure Kerberos ticket is valid for long-running jobs
            maybeRelogin();
            return userGroupInformation.doAs(action);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private void maybeRelogin() {
        if (!isAuthTypeKerberos) {
            return;
        }
        if (userGroupInformation == null) {
            return;
        }
        try {
            if (userGroupInformation.isFromKeytab()) {
                userGroupInformation.checkTGTAndReloginFromKeytab();
            }
        } catch (IOException e) {
            log.warn("Kerberos re-login from keytab failed: {}", e.getMessage());
        }
    }
}
