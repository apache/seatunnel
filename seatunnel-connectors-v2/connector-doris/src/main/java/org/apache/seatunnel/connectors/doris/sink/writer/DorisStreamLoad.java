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

package org.apache.seatunnel.connectors.doris.sink.writer;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.rest.models.RespContent;
import org.apache.seatunnel.connectors.doris.sink.HttpPutBuilder;
import org.apache.seatunnel.connectors.doris.sink.LoadStatus;
import org.apache.seatunnel.connectors.doris.util.ResponseUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.seatunnel.connectors.doris.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.seatunnel.connectors.doris.sink.writer.LoadConstants.LINE_DELIMITER_KEY;
import static org.apache.seatunnel.connectors.doris.util.ResponseUtil.LABEL_EXIST_PATTERN;

/** load data to doris. */
@Slf4j
public class DorisStreamLoad implements Serializable {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int HTTP_TEMPORARY_REDIRECT = 200;
    private final LabelGenerator labelGenerator;
    private final byte[] lineDelimiter;
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private static final String ABORT_URL_PATTERN = "http://%s/api/%s/_stream_load_2pc";
    private static final String JOB_EXIST_FINISHED = "FINISHED";
    private final String loadUrlStr;
    @Getter private final String hostPort;
    private final String abortUrlStr;
    private final String user;
    private final String passwd;
    @Getter private final String db;
    private final String table;
    private final boolean enable2PC;
    private final boolean enableDelete;
    private final Properties streamLoadProp;
    private final RecordStream recordStream;
    @Getter private Future<CloseableHttpResponse> pendingLoadFuture;
    private final CloseableHttpClient httpClient;
    private final ExecutorService executorService;
    private volatile boolean loadBatchFirstRecord;
    private volatile boolean loading = false;
    private String label;
    @Getter private long recordCount = 0;

    public DorisStreamLoad(
            String hostPort,
            TablePath tablePath,
            DorisConfig dorisConfig,
            LabelGenerator labelGenerator,
            CloseableHttpClient httpClient) {
        this.hostPort = hostPort;
        this.db = tablePath.getDatabaseName();
        this.table = tablePath.getTableName();
        this.user = dorisConfig.getUsername();
        this.passwd = dorisConfig.getPassword();
        this.labelGenerator = labelGenerator;
        this.loadUrlStr = String.format(LOAD_URL_PATTERN, hostPort, db, table);
        this.abortUrlStr = String.format(ABORT_URL_PATTERN, hostPort, db);
        this.enable2PC = dorisConfig.getEnable2PC();
        this.streamLoadProp = dorisConfig.getStreamLoadProps();
        this.enableDelete = dorisConfig.getEnableDelete();
        this.httpClient = httpClient;
        this.executorService =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        new ThreadFactoryBuilder().setNameFormat("stream-load-upload").build());
        this.recordStream =
                new RecordStream(dorisConfig.getBufferSize(), dorisConfig.getBufferCount());
        lineDelimiter =
                streamLoadProp.getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT).getBytes();
        loadBatchFirstRecord = true;
    }

    public void abortPreCommit(String labelSuffix, long chkID) throws Exception {
        long startChkID = chkID;
        log.info("abort for labelSuffix {}. start chkId {}.", labelSuffix, chkID);
        while (true) {
            try {
                String label = labelGenerator.generateLabel(startChkID);
                HttpPutBuilder builder = new HttpPutBuilder();
                builder.setUrl(loadUrlStr)
                        .baseAuth(user, passwd)
                        .addCommonHeader()
                        .enable2PC()
                        .setLabel(label)
                        .setEmptyEntity()
                        .addProperties(streamLoadProp);
                RespContent respContent =
                        handlePreCommitResponse(httpClient.execute(builder.build()));
                checkState("true".equals(respContent.getTwoPhaseCommit()));
                if (LoadStatus.LABEL_ALREADY_EXIST.equals(respContent.getStatus())) {
                    // label already exist and job finished
                    if (JOB_EXIST_FINISHED.equals(respContent.getExistingJobStatus())) {
                        throw new DorisConnectorException(
                                DorisConnectorErrorCode.STREAM_LOAD_FAILED,
                                "Load status is "
                                        + LoadStatus.LABEL_ALREADY_EXIST
                                        + " and load job finished, "
                                        + "change you label prefix or restore from latest savepoint!");
                    }
                    // job not finished, abort.
                    Matcher matcher = LABEL_EXIST_PATTERN.matcher(respContent.getMessage());
                    if (matcher.find()) {
                        checkState(label.equals(matcher.group(1)));
                        long txnId = Long.parseLong(matcher.group(2));
                        log.info("abort {} for exist label {}", txnId, label);
                        abortTransaction(txnId);
                    } else {
                        throw new DorisConnectorException(
                                DorisConnectorErrorCode.STREAM_LOAD_FAILED,
                                "Load Status is "
                                        + LoadStatus.LABEL_ALREADY_EXIST
                                        + ", but no txnID associated with it!"
                                        + "response: "
                                        + respContent);
                    }
                } else {
                    log.info("abort {} for check label {}.", respContent.getTxnId(), label);
                    abortTransaction(respContent.getTxnId());
                    break;
                }
                startChkID++;
            } catch (Exception e) {
                log.warn("failed to stream load data", e);
                throw e;
            }
        }
        log.info("abort for labelSuffix {} finished", labelSuffix);
    }

    public void writeRecord(byte[] record) throws IOException {
        if (loadBatchFirstRecord) {
            loadBatchFirstRecord = false;
            recordStream.startInput();
            startStreamLoad();
        } else {
            recordStream.write(lineDelimiter);
        }
        recordStream.write(record);
        recordCount++;
    }

    public String getLoadFailedMsg() {
        if (!loading) {
            return null;
        }
        if (this.getPendingLoadFuture() != null && this.getPendingLoadFuture().isDone()) {
            String errorMessage;
            try {
                errorMessage = handlePreCommitResponse(pendingLoadFuture.get()).getMessage();
            } catch (Exception e) {
                errorMessage = ExceptionUtils.getMessage(e);
            }
            recordStream.setErrorMessageByStreamLoad(errorMessage);
            return errorMessage;
        } else {
            return null;
        }
    }

    private RespContent handlePreCommitResponse(CloseableHttpResponse response) throws Exception {
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HTTP_TEMPORARY_REDIRECT && response.getEntity() != null) {
            String loadResult = EntityUtils.toString(response.getEntity());
            log.info("load Result {}", loadResult);
            return OBJECT_MAPPER.readValue(loadResult, RespContent.class);
        }
        throw new DorisConnectorException(
                DorisConnectorErrorCode.STREAM_LOAD_FAILED, response.getStatusLine().toString());
    }

    public RespContent stopLoad() throws IOException {
        loading = false;
        if (pendingLoadFuture != null) {
            log.info("stream load stopped.");
            recordStream.endInput();
            try {
                return handlePreCommitResponse(pendingLoadFuture.get());
            } catch (Exception e) {
                throw new DorisConnectorException(DorisConnectorErrorCode.STREAM_LOAD_FAILED, e);
            } finally {
                pendingLoadFuture = null;
            }
        } else {
            return null;
        }
    }

    public void startLoad(String label) {
        loadBatchFirstRecord = true;
        recordCount = 0;
        this.label = label;
        this.loading = true;
    }

    private void startStreamLoad() {
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        log.info("stream load started for {}", label);
        try {
            InputStreamEntity entity = new InputStreamEntity(recordStream);
            putBuilder
                    .setUrl(loadUrlStr)
                    .baseAuth(user, passwd)
                    .addCommonHeader()
                    .addHiddenColumns(enableDelete)
                    .setLabel(label)
                    .setEntity(entity)
                    .addProperties(streamLoadProp);
            if (enable2PC) {
                putBuilder.enable2PC();
            }
            pendingLoadFuture =
                    executorService.submit(
                            () -> {
                                log.info("start execute load");
                                return httpClient.execute(putBuilder.build());
                            });
        } catch (Exception e) {
            String err = "failed to stream load data with label: " + label;
            log.warn(err, e);
            throw e;
        }
    }

    public void abortTransaction(long txnID) throws Exception {
        HttpPutBuilder builder = new HttpPutBuilder();
        builder.setUrl(abortUrlStr)
                .baseAuth(user, passwd)
                .addCommonHeader()
                .addTxnId(txnID)
                .setEmptyEntity()
                .abort();
        CloseableHttpResponse response = httpClient.execute(builder.build());

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HTTP_TEMPORARY_REDIRECT || response.getEntity() == null) {
            log.warn("abort transaction response: " + response.getStatusLine().toString());
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.STREAM_LOAD_FAILED,
                    "Fail to abort transaction " + txnID + " with url " + abortUrlStr);
        }

        String loadResult = EntityUtils.toString(response.getEntity());
        Map<String, String> res =
                JsonUtils.parseObject(loadResult, new TypeReference<HashMap<String, String>>() {});
        if (!LoadStatus.SUCCESS.equals(res.get("status"))) {
            if (ResponseUtil.isCommitted(res.get("msg"))) {
                throw new DorisConnectorException(
                        DorisConnectorErrorCode.STREAM_LOAD_FAILED,
                        "try abort committed transaction, " + "do you recover from old savepoint?");
            }
            log.warn("Fail to abort transaction. txnId: {}, error: {}", txnID, res.get("msg"));
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
