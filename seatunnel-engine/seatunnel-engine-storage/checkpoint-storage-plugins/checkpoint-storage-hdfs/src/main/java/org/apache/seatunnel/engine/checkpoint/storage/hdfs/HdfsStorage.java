package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.AbstractCheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class HdfsStorage extends AbstractCheckpointStorage {

    public FileSystem fs;
    private static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";

    private static final String KERBEROS_KEY = "kerberos";

    @Override
    public void initStorage(Map<String, String> configuration) throws CheckpointStorageException {
        Configuration hadoopConf = new Configuration();
        if (configuration.containsKey(HdfsConstants.HDFS_DEF_FS_NAME)) {
            hadoopConf.set(HdfsConstants.HDFS_DEF_FS_NAME, configuration.get(HdfsConstants.HDFS_DEF_FS_NAME));
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
            throw new CheckpointStorageException("Failed to serialize checkpoint data", e);
        }
        //Consider file paths for different operating systems
        String fileName = getStorageParentDirectory() + state.getJobId() + "/" + getCheckPointName(state);
        try (FSDataOutputStream out = fs.create(new Path(fileName))) {
            out.write(datas);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to write checkpoint data", e);
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
                states.add(readPipelineState(file));
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
        Map<String, String> latestPipelineMap = new HashMap<>();
        fileNames.forEach(file -> {
            String filePipelineId = file.split(FILE_NAME_SPLIT)[FILE_NAME_PIPELINE_ID_INDEX];
            int fileVersion = Integer.parseInt(file.split(FILE_NAME_SPLIT)[FILE_SORT_ID_INDEX]);
            if (latestPipelineMap.containsKey(filePipelineId)) {
                int oldVersion = Integer.parseInt(latestPipelineMap.get(filePipelineId).split(FILE_NAME_SPLIT)[FILE_SORT_ID_INDEX]);
                if (fileVersion > oldVersion) {
                    latestPipelineMap.put(filePipelineId, file);
                }
            } else {
                latestPipelineMap.put(filePipelineId, file);
            }
        });
        List<PipelineState> latestPipelineStates = new ArrayList<>();
        latestPipelineMap.forEach((pipelineId, fileName) -> {
            try {
                latestPipelineStates.add(readPipelineState(fileName));
            } catch (CheckpointStorageException e) {
                log.error("Failed to read pipeline state for file: {}", fileName, e);
            }
        });
        if (latestPipelineStates.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId);
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

        AtomicReference<String> latestFileName = new AtomicReference<>(null);
        AtomicInteger latestVersion = new AtomicInteger();
        fileNames.forEach(file -> {
            int fileVersion = Integer.parseInt(file.split(FILE_NAME_SPLIT)[FILE_SORT_ID_INDEX]);
            String filePipelineId = file.split(FILE_NAME_SPLIT)[FILE_NAME_PIPELINE_ID_INDEX];
            if (pipelineId.equals(filePipelineId) && fileVersion > latestVersion.get()) {
                latestVersion.set(fileVersion);
                latestFileName.set(file);
            }
        });
        if (latestFileName.get() == null) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId + ", pipeline id is: " + pipelineId);
        }

        return readPipelineState(latestFileName.get());
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
                    pipelineStates.add(readPipelineState(file));
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
    private PipelineState readPipelineState(String fileName) throws CheckpointStorageException {
        try (FSDataInputStream in = fs.open(new Path(fileName))) {
            byte[] datas = new byte[in.available()];
            in.read(datas);
            return deserializeCheckPointData(datas);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to read checkpoint data", e);
        }
    }
}
