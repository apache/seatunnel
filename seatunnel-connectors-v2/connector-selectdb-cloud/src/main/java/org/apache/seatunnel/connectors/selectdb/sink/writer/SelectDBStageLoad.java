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

package org.apache.seatunnel.connectors.selectdb.sink.writer;

import org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig;
import org.apache.seatunnel.connectors.selectdb.exception.SelectDBConnectorErrorCode;
import org.apache.seatunnel.connectors.selectdb.exception.SelectDBConnectorException;
import org.apache.seatunnel.connectors.selectdb.rest.BaseResponse;
import org.apache.seatunnel.connectors.selectdb.rest.CopySQLUtil;
import org.apache.seatunnel.connectors.selectdb.util.HttpPutBuilder;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.seatunnel.connectors.selectdb.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.seatunnel.connectors.selectdb.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

@Slf4j
public class SelectDBStageLoad implements Serializable {
    private final LabelGenerator labelGenerator;
    private final String lineDelimiter;
    private static final String UPLOAD_URL_PATTERN = "http://%s/copy/upload";

    private final SelectDBConfig selectdbConfig;
    private String uploadUrl;
    private String hostPort;
    private final String username;
    private final String password;
    private final Properties stageLoadProps;
    private List<String> fileList = new CopyOnWriteArrayList();
    private RecordBuffer buffer;
    private long currentCheckpointID;
    private AtomicInteger fileNum;
    private ExecutorService loadExecutorService;
    private StageLoadAsyncExecutor loadAsyncExecutor;
    private ArrayBlockingQueue<RecordBuffer> queue;
    private final AtomicBoolean started;
    private AtomicReference<Throwable> exception = new AtomicReference<>(null);
    private HttpClientBuilder httpClientBuilder = HttpClients.custom().disableRedirectHandling();

    public SelectDBStageLoad(SelectDBConfig selectdbConfig, LabelGenerator labelGenerator) {
        this.selectdbConfig = selectdbConfig;
        this.hostPort = selectdbConfig.getLoadUrl();
        this.username = selectdbConfig.getUsername();
        this.password = selectdbConfig.getPassword();
        this.labelGenerator = labelGenerator;
        this.uploadUrl = String.format(UPLOAD_URL_PATTERN, hostPort);
        this.stageLoadProps = selectdbConfig.getStageLoadProps();
        this.lineDelimiter = stageLoadProps.getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT);
        this.fileNum = new AtomicInteger();
        this.buffer = new RecordBuffer(lineDelimiter);
        this.queue = new ArrayBlockingQueue<>(selectdbConfig.getFlushQueueSize());
        this.loadAsyncExecutor = new StageLoadAsyncExecutor();
        this.loadExecutorService =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1),
                        new DefaultThreadFactory("upload-executor"),
                        new ThreadPoolExecutor.AbortPolicy());
        this.started = new AtomicBoolean(true);
        this.loadExecutorService.execute(loadAsyncExecutor);
    }

    public String getHostPort() {
        return hostPort;
    }

    public List<String> getFileList() {
        return fileList;
    }

    public void clearFileList() {
        this.fileNum.set(0);
        fileList.clear();
    }

    /**
     * write record into cache.
     *
     * @param record
     * @throws IOException
     */
    public void writeRecord(byte[] record) throws InterruptedException {
        buffer.insert(new String(record, StandardCharsets.UTF_8));
        if (buffer.getBufferSizeBytes() >= selectdbConfig.getBufferSize()
                || (selectdbConfig.getBufferCount() != 0
                        && buffer.getNumOfRecords() >= selectdbConfig.getBufferCount())) {
            flush(false);
        }
    }

    public void flush(boolean waitUtilDone) throws InterruptedException {
        checkFlushException();
        if (buffer == null) {
            return;
        }
        String fileName =
                labelGenerator.generateLabel(currentCheckpointID, fileNum.getAndIncrement());
        buffer.setFileName(fileName);
        RecordBuffer tmpBuff = buffer;
        log.info("flush buffer to queue, actual queue size {}", queue.size());
        offer(tmpBuff);
        if (waitUtilDone) {
            waitAsyncLoadFinish();
        }
        this.buffer = new RecordBuffer(this.lineDelimiter);
    }

    private void offer(RecordBuffer buffer) throws InterruptedException {
        checkFlushException();
        if (!queue.offer(buffer, 600 * 1000, TimeUnit.MILLISECONDS)) {
            throw new SelectDBConnectorException(
                    SelectDBConnectorErrorCode.STAGE_LOAD_FAILED,
                    "offer data to queue timeout, exceed ");
        }
    }

    private void checkFlushException() {
        if (exception.get() != null) {
            throw new SelectDBConnectorException(
                    SelectDBConnectorErrorCode.STAGE_LOAD_FAILED, exception.get());
        }
    }

    private void waitAsyncLoadFinish() throws InterruptedException {
        for (int i = 0; i < selectdbConfig.getFlushQueueSize() + 1; i++) {
            offer(new RecordBuffer());
        }
    }

    public void close() {
        this.started.set(false);
        this.loadExecutorService.shutdown();
    }

    public void setCurrentCheckpointID(long currentCheckpointID) {
        this.currentCheckpointID = currentCheckpointID;
    }

    class StageLoadAsyncExecutor implements Runnable {
        @Override
        public void run() {
            log.info("StageLoadAsyncExecutor start");
            while (started.get()) {
                try {
                    RecordBuffer buffer = queue.poll(2000L, TimeUnit.MILLISECONDS);
                    if (buffer != null && buffer.getFileName() != null) {
                        uploadToStorage(buffer.getFileName(), buffer);
                        fileList.add(buffer.getFileName());
                        if (!selectdbConfig.isEnable2PC()) {
                            CopySQLBuilder copySQLBuilder =
                                    new CopySQLBuilder(selectdbConfig, fileList);
                            String copySql = copySQLBuilder.buildCopySQL();
                            CopySQLUtil.copyFileToDatabase(
                                    selectdbConfig,
                                    selectdbConfig.getClusterName(),
                                    copySql,
                                    hostPort);
                            log.info("clear the file list {}", fileList);
                            clearFileList();
                        }
                    }
                } catch (Exception e) {
                    log.error("worker running error", e);
                    exception.set(e);
                    break;
                }
            }
            log.info("StageLoadAsyncExecutor stop");
        }

        /** upload to storage */
        public void uploadToStorage(String fileName, RecordBuffer buffer) {
            long start = System.currentTimeMillis();
            log.info("file write started for {}", fileName);
            String address = getUploadAddress(fileName);
            log.info("redirect to internalStage address:{}", address);
            uploadToInternalStage(address, buffer.getData().getBytes(StandardCharsets.UTF_8));
            log.info(
                    "upload file {} finished, record {} size {}, cost {}ms ",
                    fileName,
                    buffer.getNumOfRecords(),
                    buffer.getBufferSizeBytes(),
                    System.currentTimeMillis() - start);
        }

        public BaseResponse uploadToInternalStage(String address, byte[] data)
                throws SelectDBConnectorException {
            ByteArrayEntity entity = new ByteArrayEntity(data);
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder.setUrl(address).addCommonHeader().setEntity(entity);
            HttpPut httpPut = putBuilder.build();
            try {
                try (CloseableHttpResponse response = httpClientBuilder.build().execute(httpPut)) {
                    final int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode == 200 && response.getEntity() != null) {
                        String loadResult = EntityUtils.toString(response.getEntity());
                        if (loadResult == null || loadResult.isEmpty()) {
                            // upload finished
                            return null;
                        }
                        throw new SelectDBConnectorException(
                                SelectDBConnectorErrorCode.STAGE_LOAD_FAILED,
                                "upload file failed: " + response.getStatusLine().toString());
                    }
                    throw new SelectDBConnectorException(
                            SelectDBConnectorErrorCode.STAGE_LOAD_FAILED,
                            "upload file error: " + response.getStatusLine().toString());
                }
            } catch (IOException ex) {
                throw new SelectDBConnectorException(
                        SelectDBConnectorErrorCode.STAGE_LOAD_FAILED,
                        "Failed to upload data to internal stage",
                        ex);
            }
        }

        /** Get the redirected s3 address */
        public String getUploadAddress(String fileName) throws SelectDBConnectorException {
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder
                    .setUrl(uploadUrl)
                    .addFileName(fileName)
                    .addCommonHeader()
                    .setEmptyEntity()
                    .baseAuth(username, password);
            try {
                try (CloseableHttpResponse execute =
                        httpClientBuilder.build().execute(putBuilder.build())) {
                    int statusCode = execute.getStatusLine().getStatusCode();
                    String reason = execute.getStatusLine().getReasonPhrase();
                    if (statusCode == 307) {
                        Header location = execute.getFirstHeader("location");
                        String uploadAddress = location.getValue();
                        return uploadAddress;
                    } else {
                        HttpEntity entity = execute.getEntity();
                        String result = entity == null ? null : EntityUtils.toString(entity);
                        String errMsg =
                                String.format(
                                        "Failed to get internalStage address, status {}, reason {}, response {}",
                                        statusCode,
                                        reason,
                                        result);
                        throw new SelectDBConnectorException(
                                SelectDBConnectorErrorCode.STAGE_LOAD_FAILED, errMsg);
                    }
                }
            } catch (IOException e) {
                throw new SelectDBConnectorException(
                        SelectDBConnectorErrorCode.STAGE_LOAD_FAILED,
                        "get internalStage address error",
                        e);
            }
        }
    }

    static class DefaultThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        DefaultThreadFactory(String name) {
            namePrefix = "pool-" + poolNumber.getAndIncrement() + "-" + name + "-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
            t.setDaemon(false);
            return t;
        }
    }
}
