/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.localfile;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.shade.org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.AbstractChunkSupportStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants.STORAGE_NAME_SPACE;

@Slf4j
public class LocalFileStorage extends AbstractChunkSupportStorage {

    private static final String[] FILE_EXTENSIONS = new String[] {FILE_FORMAT};

    private static final String DEFAULT_WINDOWS_OS_NAME_SPACE =
            "C:\\ProgramData\\seatunnel\\checkpoint\\";

    private static final String DEFAULT_LINUX_OS_NAME_SPACE = "/tmp/seatunnel/checkpoint/";

    public LocalFileStorage(Map<String, String> configuration) {
        initStorage(configuration);
    }

    @Override
    public void initStorage(Map<String, String> configuration) {
        if (MapUtils.isEmpty(configuration)) {
            setDefaultStorageSpaceByOSName();
            return;
        }
        if (StringUtils.isNotBlank(configuration.get(STORAGE_NAME_SPACE))) {
            setStorageNameSpace(configuration.get(STORAGE_NAME_SPACE));
        }
    }

    /** set default storage root directory */
    private void setDefaultStorageSpaceByOSName() {
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            setStorageNameSpace(DEFAULT_WINDOWS_OS_NAME_SPACE);
        } else {
            setStorageNameSpace(DEFAULT_LINUX_OS_NAME_SPACE);
        }
    }

    @Override
    public String storeCheckPoint(PipelineState state) throws CheckpointStorageException {
        byte[] datas;
        try {
            datas = serializeCheckPointData(state);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to serialize checkpoint data", e);
        }
        // Consider file paths for different operating systems
        String fileName =
                getStorageParentDirectory()
                        + state.getJobId()
                        + File.separator
                        + getCheckPointName(state);

        File file = new File(fileName);
        try {
            FileUtils.touch(file);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to create checkpoint file " + fileName, e);
        }

        try {
            FileUtils.writeByteArrayToFile(file, datas);
        } catch (IOException e) {
            throw new CheckpointStorageException(
                    "Failed to write checkpoint data to file " + fileName, e);
        }

        return fileName;
    }

    @Override
    public List<PipelineState> getAllCheckpoints(String jobId) throws CheckpointStorageException {
        File filePath = new File(getStorageParentDirectory() + jobId);
        if (!filePath.exists()) {
            return new ArrayList<>();
        }

        Collection<File> fileList;
        try {
            fileList = FileUtils.listFiles(filePath, FILE_EXTENSIONS, true);
        } catch (Exception e) {
            throw new CheckpointStorageException(
                    "Failed to get all checkpoints for job " + jobId, e);
        }
        if (fileList.isEmpty()) {
            log.info("No checkpoint found for this job, the job id is: " + jobId);
            return new ArrayList<>();
        }
        List<PipelineState> states = new ArrayList<>();
        fileList.forEach(
                file -> {
                    try {
                        byte[] data = FileUtils.readFileToByteArray(file);
                        states.add(deserializeCheckPointData(data));
                    } catch (IOException e) {
                        log.error(
                                "Failed to read checkpoint data from file "
                                        + file.getAbsolutePath(),
                                e);
                    }
                });
        return states;
    }

    @Override
    public List<PipelineState> getLatestCheckpoint(String jobId) throws CheckpointStorageException {
        String parentPath = getStorageParentDirectory() + jobId;
        Collection<File> fileList = new ArrayList<>();
        try {
            fileList = FileUtils.listFiles(new File(parentPath), FILE_EXTENSIONS, false);
        } catch (Exception e) {
            if (!(e.getCause() instanceof NoSuchFileException)) {
                throw new CheckpointStorageException(ExceptionUtils.getMessage(e));
            }
        }
        if (fileList.isEmpty()) {
            log.info("No checkpoint found for this  job, the job id is: " + jobId);
            return new ArrayList<>();
        }
        Map<String, File> fileMap =
                fileList.stream()
                        .collect(
                                Collectors.toMap(
                                        File::getName, Function.identity(), (v1, v2) -> v2));
        Set<String> latestPipelines = getLatestPipelineNames(fileMap.keySet());
        List<PipelineState> latestPipelineFiles = new ArrayList<>(latestPipelines.size());
        latestPipelines.forEach(
                fileName -> {
                    File file = fileMap.get(fileName);
                    try {
                        byte[] data = FileUtils.readFileToByteArray(file);
                        latestPipelineFiles.add(deserializeCheckPointData(data));
                    } catch (IOException e) {
                        log.error(
                                "Failed to read checkpoint data from file "
                                        + file.getAbsolutePath(),
                                e);
                    }
                });
        if (latestPipelineFiles.isEmpty()) {
            log.info("No checkpoint found for this job,  the job id:{} " + jobId);
        }
        return latestPipelineFiles;
    }

    @Override
    public PipelineState getLatestCheckpointByJobIdAndPipelineId(String jobId, String pipelineId)
            throws CheckpointStorageException {

        String parentPath = getStorageParentDirectory() + jobId;
        Collection<File> fileList = new ArrayList<>();
        try {
            fileList = FileUtils.listFiles(new File(parentPath), FILE_EXTENSIONS, false);
        } catch (Exception e) {
            if (!(e.getCause() instanceof NoSuchFileException)) {
                throw new CheckpointStorageException(ExceptionUtils.getMessage(e));
            }
        }
        if (fileList.isEmpty()) {
            log.info("No checkpoint found for job, job id is: " + jobId);
            return null;
        }
        List<String> fileNames = fileList.stream().map(File::getName).collect(Collectors.toList());

        String latestFileName =
                getLatestCheckpointFileNameByJobIdAndPipelineId(fileNames, pipelineId);

        AtomicReference<PipelineState> latestFile = new AtomicReference<>(null);
        fileList.forEach(
                file -> {
                    String fileName = file.getName();
                    if (fileName.equals(latestFileName)) {
                        try {
                            byte[] data = FileUtils.readFileToByteArray(file);
                            latestFile.set(deserializeCheckPointData(data));
                        } catch (IOException e) {
                            log.error(
                                    "read checkpoint data from file " + file.getAbsolutePath(), e);
                        }
                    }
                });

        if (latestFile.get() == null) {
            log.info(
                    "No checkpoint found for this job, the job id is: "
                            + jobId
                            + ", pipeline id is: "
                            + pipelineId);
            return null;
        }
        return latestFile.get();
    }

    @Override
    public List<PipelineState> getCheckpointsByJobIdAndPipelineId(String jobId, String pipelineId)
            throws CheckpointStorageException {
        String parentPath = getStorageParentDirectory() + jobId;
        Collection<File> fileList = new ArrayList<>();
        try {
            fileList = FileUtils.listFiles(new File(parentPath), FILE_EXTENSIONS, false);
        } catch (Exception e) {
            if (!(e.getCause() instanceof NoSuchFileException)) {
                throw new CheckpointStorageException(ExceptionUtils.getMessage(e));
            }
        }
        if (fileList.isEmpty()) {
            log.info("No checkpoint found for this job, the job id is: " + jobId);
            return new ArrayList<>();
        }

        List<PipelineState> pipelineStates = new ArrayList<>();
        fileList.forEach(
                file -> {
                    String filePipelineId = getPipelineIdByFileName(file.getName());
                    if (pipelineId.equals(filePipelineId)) {
                        try {
                            byte[] data = FileUtils.readFileToByteArray(file);
                            pipelineStates.add(deserializeCheckPointData(data));
                        } catch (IOException e) {
                            log.error(
                                    "Failed to read checkpoint data from file "
                                            + file.getAbsolutePath(),
                                    e);
                        }
                    }
                });
        return pipelineStates;
    }

    @Override
    public void deleteCheckpoint(String jobId) {
        String jobPath = getStorageParentDirectory() + jobId;
        File file = new File(jobPath);
        try {
            FileUtils.deleteDirectory(file);
        } catch (IOException e) {
            log.warn("Failed to delete checkpoint directory " + jobPath, e);
        }
    }

    @Override
    public PipelineState getCheckpoint(String jobId, String pipelineId, String checkpointId)
            throws CheckpointStorageException {
        String parentPath = getStorageParentDirectory() + jobId;
        Collection<File> fileList = new ArrayList<>();
        try {
            fileList = FileUtils.listFiles(new File(parentPath), FILE_EXTENSIONS, false);
        } catch (Exception e) {
            if (!(e.getCause() instanceof NoSuchFileException)) {
                throw new CheckpointStorageException(ExceptionUtils.getMessage(e));
            }
        }
        if (fileList.isEmpty()) {
            log.info("No checkpoint found for this job,  the job id is: " + jobId);
            return null;
        }
        for (File file : fileList) {
            String fileName = file.getName();
            if (pipelineId.equals(getPipelineIdByFileName(fileName))
                    && checkpointId.equals(getCheckpointIdByFileName(fileName))) {
                try {
                    byte[] data = FileUtils.readFileToByteArray(file);
                    return deserializeCheckPointData(data);
                } catch (Exception e) {
                    log.error(
                            "Failed to delete checkpoint {} for job {}, pipeline {}",
                            checkpointId,
                            jobId,
                            pipelineId,
                            e);
                }
            }
        }
        throw new CheckpointStorageException(
                String.format(
                        "No checkpoint found, job(%s), pipeline(%s), checkpoint(%s)",
                        jobId, pipelineId, checkpointId));
    }

    @Override
    public synchronized void deleteCheckpoint(String jobId, String pipelineId, String checkpointId)
            throws CheckpointStorageException {
        String parentPath = getStorageParentDirectory() + jobId;
        Collection<File> fileList = new ArrayList<>();
        try {
            fileList = FileUtils.listFiles(new File(parentPath), FILE_EXTENSIONS, false);
        } catch (Exception e) {
            if (!(e.getCause() instanceof NoSuchFileException)) {
                throw new CheckpointStorageException(ExceptionUtils.getMessage(e));
            }
        }
        if (fileList.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job " + jobId);
        }
        fileList.forEach(
                file -> {
                    String fileName = file.getName();
                    if (pipelineId.equals(getPipelineIdByFileName(fileName))
                            && checkpointId.equals(getCheckpointIdByFileName(fileName))) {
                        try {
                            FileUtils.delete(file);
                        } catch (Exception e) {
                            log.error(
                                    "Failed to delete checkpoint {} for job {}, pipeline {}",
                                    checkpointId,
                                    jobId,
                                    pipelineId,
                                    e);
                        }
                    }
                });
    }

    @Override
    public void deleteCheckpoint(String jobId, String pipelineId, List<String> checkpointIdList)
            throws CheckpointStorageException {
        String parentPath = getStorageParentDirectory() + jobId;
        Collection<File> fileList = new ArrayList<>();
        try {
            fileList = FileUtils.listFiles(new File(parentPath), FILE_EXTENSIONS, false);
        } catch (Exception e) {
            if (!(e.getCause() instanceof NoSuchFileException)) {
                throw new CheckpointStorageException(ExceptionUtils.getMessage(e));
            }
        }
        if (fileList.isEmpty()) {
            throw new CheckpointStorageException(
                    "No checkpoint found for job, job id is: " + jobId);
        }
        fileList.forEach(
                file -> {
                    String fileName = file.getName();
                    String checkpointIdByFileName = getCheckpointIdByFileName(fileName);
                    if (pipelineId.equals(getPipelineIdByFileName(fileName))
                            && checkpointIdList.contains(checkpointIdByFileName)) {
                        try {
                            FileUtils.delete(file);
                        } catch (Exception e) {
                            log.error(
                                    "Failed to delete checkpoint {} for job {}, pipeline {}",
                                    checkpointIdByFileName,
                                    jobId,
                                    pipelineId,
                                    e);
                        }
                    }
                });
    }

    private Path resolve(String relativePath) {
        return Paths.get(getStorageParentDirectory(), relativePath);
    }

    @Override
    protected void writeNewFile(String relativePath, byte[] data) throws IOException {
        Path file = resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.write(file, data, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    /**
     * Creates {@code target} from {@code source} only if it does not exist yet. A hard link is
     * created atomically and fails if the target exists, unlike a POSIX rename, which silently
     * replaces it. The source link is then removed on a best-effort basis.
     */
    @Override
    protected void moveIfAbsent(String source, String target) throws IOException {
        Path from = resolve(source);
        Path to = resolve(target);
        try {
            Files.createLink(to, from);
        } catch (UnsupportedOperationException e) {
            // file systems without hard links: move fails if the target exists (not atomic, but
            // there is a single committer per checkpoint)
            Files.move(from, to);
            return;
        }
        try {
            Files.deleteIfExists(from);
        } catch (IOException e) {
            // the target is published; a leftover temporary file is ignored by readers
            log.warn("Failed to delete temporary checkpoint file {}", from, e);
        }
    }

    @Override
    protected void deleteIfExists(String relativePath) throws IOException {
        Files.deleteIfExists(resolve(relativePath));
    }

    @Override
    protected byte[] readFile(String relativePath) throws IOException {
        return Files.readAllBytes(resolve(relativePath));
    }

    @Override
    protected boolean fileExists(String relativePath) {
        return Files.isRegularFile(resolve(relativePath));
    }

    @Override
    protected List<String> listChildDirectories(String relativePath) throws IOException {
        Path dir = resolve(relativePath);
        List<String> names = new ArrayList<>();
        if (!Files.isDirectory(dir)) {
            return names;
        }
        try (DirectoryStream<Path> children = Files.newDirectoryStream(dir, Files::isDirectory)) {
            for (Path child : children) {
                names.add(child.getFileName().toString());
            }
        }
        return names;
    }

    @Override
    protected List<String> listFileNames(String relativePath) throws IOException {
        Path dir = resolve(relativePath);
        List<String> names = new ArrayList<>();
        if (!Files.isDirectory(dir)) {
            return names;
        }
        try (DirectoryStream<Path> children = Files.newDirectoryStream(dir, Files::isRegularFile)) {
            for (Path child : children) {
                names.add(child.getFileName().toString());
            }
        }
        return names;
    }
}
