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

package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import static org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants.STORAGE_NAME_SPACE;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.AbstractCheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class HdfsStorage extends AbstractCheckpointStorage {

    public FileSystem fs;
    private static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";

    private static final String KERBEROS_KEY = "kerberos";

    public HdfsStorage(Map<String, String> configuration) throws CheckpointStorageException {
        this.initStorage(configuration);
    }

    @Override
    public void initStorage(Map<String, String> configuration) throws CheckpointStorageException {
        Configuration hadoopConf = new Configuration();
        if (configuration.containsKey(HdfsConstants.HDFS_DEF_FS_NAME)) {
            hadoopConf.set(HdfsConstants.HDFS_DEF_FS_NAME, configuration.get(HdfsConstants.HDFS_DEF_FS_NAME));
        }
        if (StringUtils.isNotBlank(configuration.get(STORAGE_NAME_SPACE))) {
            setStorageNameSpace(configuration.get(STORAGE_NAME_SPACE));
        }
        // todo support other config configurations
        if (configuration.containsKey(HdfsConstants.KERBEROS_PRINCIPAL) && configuration.containsKey(HdfsConstants.KERBEROS_KEYTAB_FILE_PATH)) {
            String kerberosPrincipal = configuration.get(HdfsConstants.KERBEROS_PRINCIPAL);
            String kerberosKeytabFilePath = configuration.get(HdfsConstants.KERBEROS_KEYTAB_FILE_PATH);
            if (StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
                hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, KERBEROS_KEY);
                authenticateKerberos(kerberosPrincipal, kerberosKeytabFilePath, hadoopConf);
            }
        }
        JobConf jobConf = new JobConf(hadoopConf);
        try {
            fs = FileSystem.get(jobConf);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to get file system", e);
        }

    }

    @Override
    public String storeCheckPoint(PipelineState state) throws CheckpointStorageException {
        byte[] datas;
        try {
            datas = serializeCheckPointData(state);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to serialize checkpoint data,state is :" + state, e);
        }
        String fileName = getStorageParentDirectory() + state.getJobId() + "/" + getCheckPointName(state);
        try (FSDataOutputStream out = fs.create(new Path(fileName))) {
            out.write(datas);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to write checkpoint data, file name is: " + fileName, e);
        }
        return fileName;
    }

    @Override
    public List<PipelineState> getAllCheckpoints(String jobId) throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId);
        }
        List<PipelineState> states = new ArrayList<>();
        fileNames.forEach(file -> {
            try {
                states.add(readPipelineState(file, jobId));
            } catch (CheckpointStorageException e) {
                log.error("Failed to read checkpoint data from file: " + file, e);
            }
        });
        if (states.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId);
        }
        return states;
    }

    @Override
    public List<PipelineState> getLatestCheckpoint(String jobId) throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId);
        }
        Set<String> latestPipelineNames = getLatestPipelineNames(fileNames);
        List<PipelineState> latestPipelineStates = new ArrayList<>();
        latestPipelineNames.forEach(fileName -> {
            try {
                latestPipelineStates.add(readPipelineState(fileName, jobId));
            } catch (CheckpointStorageException e) {
                log.error("Failed to read pipeline state for file: {}", fileName, e);
            }
        });

        if (latestPipelineStates.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id:{} " + jobId);
        }
        return latestPipelineStates;
    }

    @Override
    public PipelineState getLatestCheckpointByJobIdAndPipelineId(String jobId, String pipelineId) throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId);
        }

        String latestFileName = getLatestCheckpointFileNameByJobIdAndPipelineId(fileNames, pipelineId);
        if (latestFileName == null) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId + ", pipeline id is: " + pipelineId);
        }
        return readPipelineState(latestFileName, jobId);
    }

    @Override
    public List<PipelineState> getCheckpointsByJobIdAndPipelineId(String jobId, String pipelineId) throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId);
        }

        List<PipelineState> pipelineStates = new ArrayList<>();
        fileNames.forEach(file -> {
            String filePipelineId = file.split(FILE_NAME_SPLIT)[FILE_NAME_PIPELINE_ID_INDEX];
            if (pipelineId.equals(filePipelineId)) {
                try {
                    pipelineStates.add(readPipelineState(file, jobId));
                } catch (Exception e) {
                    log.error("Failed to read checkpoint data from file " + file, e);
                }
            }
        });
        return pipelineStates;
    }

    @Override
    public void deleteCheckpoint(String jobId) {
        String jobPath = getStorageParentDirectory() + jobId;
        try {
            fs.delete(new Path(jobPath), true);
        } catch (IOException e) {
            log.error("Failed to delete checkpoint for job {}", jobId, e);
        }
    }

    /**
     * Authenticate kerberos
     *
     * @param kerberosPrincipal      kerberos principal
     * @param kerberosKeytabFilePath kerberos keytab file path
     * @param hdfsConf               hdfs configuration
     * @throws CheckpointStorageException authentication exception
     */
    private void authenticateKerberos(String kerberosPrincipal, String kerberosKeytabFilePath, Configuration hdfsConf) throws CheckpointStorageException {
        UserGroupInformation.setConfiguration(hdfsConf);
        try {
            UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to login user from keytab : " + kerberosKeytabFilePath + " and kerberos principal : " + kerberosPrincipal, e);
        }
    }

    private List<String> getFileNames(String path) throws CheckpointStorageException {
        try {
            RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs.listFiles(new Path(path), false);
            List<String> fileNames = new ArrayList<>();
            while (fileStatusRemoteIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
                fileNames.add(fileStatus.getPath().getName());
            }
            return fileNames;
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to list files from names" + path, e);
        }
    }

    /**
     * Get checkpoint name
     *
     * @param fileName file name
     * @return checkpoint data
     */
    private PipelineState readPipelineState(String fileName, String jobId) throws CheckpointStorageException {
        fileName = getStorageParentDirectory() + jobId + DEFAULT_CHECKPOINT_FILE_PATH_SPLIT + fileName;
        try (FSDataInputStream in = fs.open(new Path(fileName))) {
            byte[] datas = new byte[in.available()];
            in.read(datas);
            return deserializeCheckPointData(datas);
        } catch (IOException e) {
            throw new CheckpointStorageException(String.format("Failed to read checkpoint data, file name is %s,job id is %s", fileName, jobId), e);
        }
    }
}
