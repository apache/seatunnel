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

package org.apache.seatunnel.connector.selectdb.sink.writer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.seatunnel.connector.selectdb.config.SelectDBConfig;
import org.apache.seatunnel.connector.selectdb.exception.SelectDBConnectorErrorCode;
import org.apache.seatunnel.connector.selectdb.exception.SelectDBConnectorException;
import org.apache.seatunnel.connector.selectdb.rest.BaseResponse;
import org.apache.seatunnel.connector.selectdb.util.HttpUtil;
import org.apache.seatunnel.connector.selectdb.util.HttpPutBuilder;
import org.apache.seatunnel.connector.selectdb.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.seatunnel.connector.selectdb.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.seatunnel.connector.selectdb.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

public class SelectDBCopyInto implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SelectDBCopyInto.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final LabelGenerator labelGenerator;
    private final byte[] lineDelimiter;
    private static final String UPLOAD_URL_PATTERN = "http://%s/copy/upload";

    private String uploadUrl;
    private String hostPort;
    private final String user;
    private final String passwd;
    private final String db;
    private final String table;
    private final boolean enable2PC;
    private final Properties streamLoadProp;
    private final RecordStream recordStream;
    private Future<CloseableHttpResponse> pendingLoadFuture;
    private final CloseableHttpClient httpClient;
    private final ExecutorService executorService;
    private boolean loadBatchFirstRecord;
    private List<String> fileList = new CopyOnWriteArrayList();

    private String fileName;

    public SelectDBCopyInto(SelectDBConfig selectdbConfig,
                            LabelGenerator labelGenerator,
                            CloseableHttpClient httpClient) {
        this.hostPort = selectdbConfig.getLoadUrl();
        String[] tableInfo = selectdbConfig.getTableIdentifier().split("\\.");
        this.db = tableInfo[0];
        this.table = tableInfo[1];
        this.user = selectdbConfig.getUsername();
        this.passwd = selectdbConfig.getPassword();
        this.labelGenerator = labelGenerator;
        this.uploadUrl = String.format(UPLOAD_URL_PATTERN, hostPort);
        this.enable2PC = selectdbConfig.getEnable2PC();
        this.streamLoadProp = selectdbConfig.getStreamLoadProps();
        this.httpClient = httpClient;
        this.executorService = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat("file-load-upload").build());
        this.recordStream = new RecordStream(selectdbConfig.getBufferSize(), selectdbConfig.getBufferCount());
        lineDelimiter = streamLoadProp.getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT).getBytes();
        loadBatchFirstRecord = true;
    }

    public String getDb() {
        return db;
    }

    public String getHostPort() {
        return hostPort;
    }

    public Future<CloseableHttpResponse> getPendingLoadFuture() {
        return pendingLoadFuture;
    }

    public String getFileName() {
        return fileName;
    }

    public List<String> getFileList() {
        return fileList;
    }

    public void clearFileList() {
        fileList.clear();
    }

    /**
     * write record into stream.
     *
     * @param record
     * @throws IOException
     */
    public void writeRecord(byte[] record) throws IOException {
        if (loadBatchFirstRecord) {
            loadBatchFirstRecord = false;
        } else {
            recordStream.write(lineDelimiter);
        }
        recordStream.write(record);
    }

    @VisibleForTesting
    public RecordStream getRecordStream() {
        return recordStream;
    }

    public BaseResponse<HashMap<String, String>> handleResponse(CloseableHttpResponse response) throws IOException {
        try {
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200 && response.getEntity() != null) {
                String loadResult = EntityUtils.toString(response.getEntity());
                if (StringUtil.isNullOrWhitespaceOnly(loadResult)) {
                    return null;
                }
                LOG.info("response result {}", loadResult);
                BaseResponse<HashMap<String, String>> baseResponse = OBJECT_MAPPER.readValue(loadResult, new TypeReference<BaseResponse<HashMap<String, String>>>() {
                });
                if (baseResponse.getCode() == 0) {
                    return baseResponse;
                } else {
                    throw new RuntimeException("upload file error: " + baseResponse.getMsg());
                }
            }
            throw new RuntimeException("upload file error: " + response.getStatusLine().toString());
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    public void stopLoad() throws IOException {
        recordStream.endInput();
        LOG.info("file {} write stopped.", fileName);
        Preconditions.checkState(pendingLoadFuture != null);
        try {
            handleResponse(pendingLoadFuture.get());
            LOG.info("upload file {} finished", fileName);
            fileList.add(fileName);
        } catch (Exception e) {
            throw new SelectDBConnectorException(SelectDBConnectorErrorCode.UPLOAD_FAILED, e);
        }
    }

    /**
     * start write data for new checkpoint.
     *
     * @param fileName
     * @throws IOException
     */
    public void startLoad(String fileName) throws IOException {
        this.fileName = fileName;
        loadBatchFirstRecord = true;
        recordStream.startInput();
        LOG.info("file write started for {}", fileName);
        try {
            String address = getUploadAddress(fileName);
            LOG.info("redirect to s3 address:{}", address);
            InputStreamEntity entity = new InputStreamEntity(recordStream);
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder.setUrl(address)
                    .addCommonHeader()
                    .setEntity(entity);
            pendingLoadFuture = executorService.submit(() -> {
                LOG.info("start execute load {}", fileName);
                return new HttpUtil().getHttpClient().execute(putBuilder.build());
//                return httpClient.execute(putBuilder.build());
            });
        } catch (Exception e) {
            String err = "failed to write data with fileName: " + fileName;
            LOG.warn(err, e);
            throw e;
        }
    }

    /**
     * Get the redirected s3 address
     */
    public String getUploadAddress(String fileName) throws IOException {
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        putBuilder.setUrl(uploadUrl)
                .addFileName(fileName)
                .addCommonHeader()
                .setEmptyEntity()
                .baseAuth(user, passwd);

        try (CloseableHttpResponse execute = httpClient.execute(putBuilder.build())) {
            int statusCode = execute.getStatusLine().getStatusCode();
            String reason = execute.getStatusLine().getReasonPhrase();
            if (statusCode == 307) {
                Header location = execute.getFirstHeader("location");
                String uploadAddress = location.getValue();
                return uploadAddress;
            } else {
                HttpEntity entity = execute.getEntity();
                String result = entity == null ? null : EntityUtils.toString(entity);
                LOG.error("Failed get the redirected address, status {}, reason {}, response {}", statusCode, reason, result);
                throw new RuntimeException("Could not get the redirected address.");
            }
        }
    }

    public void close() throws IOException {
        if (null != httpClient) {
            try {
                httpClient.close();
            } catch (IOException e) {
                throw new IOException("Closing httpClient failed.", e);
            }
        }
        if (null != executorService) {
            executorService.shutdownNow();
        }
    }
}
