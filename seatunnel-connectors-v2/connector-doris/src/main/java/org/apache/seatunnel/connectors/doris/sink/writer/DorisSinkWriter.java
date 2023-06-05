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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.rest.RestService;
import org.apache.seatunnel.connectors.doris.rest.models.BackendV2;
import org.apache.seatunnel.connectors.doris.rest.models.RespContent;
import org.apache.seatunnel.connectors.doris.serialize.DorisSerializer;
import org.apache.seatunnel.connectors.doris.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.doris.sink.LoadStatus;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitInfo;
import org.apache.seatunnel.connectors.doris.util.HttpUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
public class DorisSinkWriter implements SinkWriter<SeaTunnelRow, DorisCommitInfo, DorisSinkState> {
    private static final int INITIAL_DELAY = 200;
    private static final int CONNECT_TIMEOUT = 1000;
    private static final List<String> DORIS_SUCCESS_STATUS =
            new ArrayList<>(Arrays.asList(LoadStatus.SUCCESS, LoadStatus.PUBLISH_TIMEOUT));
    private long lastCheckpointId;
    private DorisStreamLoad dorisStreamLoad;
    volatile boolean loading;
    private final DorisConfig dorisConfig;
    private final String labelPrefix;
    private final LabelGenerator labelGenerator;
    private final int intervalTime;
    private final DorisSinkState dorisSinkState;
    private final DorisSerializer serializer;
    private final transient ScheduledExecutorService scheduledExecutorService;
    private transient Thread executorThread;
    private transient volatile Exception loadException = null;
    private List<BackendV2.BackendRowV2> backends;
    private long pos;

    public DorisSinkWriter(
            SinkWriter.Context context,
            List<DorisSinkState> state,
            SeaTunnelRowType seaTunnelRowType,
            Config pluginConfig,
            String jobId) {
        this.dorisConfig = DorisConfig.loadConfig(pluginConfig);
        this.lastCheckpointId = state.size() != 0 ? state.get(0).getCheckpointId() : 0;
        log.info("restore checkpointId {}", lastCheckpointId);
        log.info("labelPrefix " + dorisConfig.getLabelPrefix());
        this.dorisSinkState = new DorisSinkState(dorisConfig.getLabelPrefix(), lastCheckpointId);
        this.labelPrefix =
                dorisConfig.getLabelPrefix() + "_" + jobId + "_" + context.getIndexOfSubtask();
        this.labelGenerator = new LabelGenerator(labelPrefix, dorisConfig.getEnable2PC());
        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(
                        1, new ThreadFactoryBuilder().setNameFormat("stream-load-check").build());
        this.serializer = createSerializer(dorisConfig, seaTunnelRowType);
        this.intervalTime = dorisConfig.getCheckInterval();
        this.loading = false;
    }

    public void initializeLoad(List<DorisSinkState> state) throws IOException {
        this.backends = RestService.getBackendsV2(dorisConfig, log);
        String backend = getAvailableBackend();
        try {
            this.dorisStreamLoad =
                    new DorisStreamLoad(
                            backend, dorisConfig, labelGenerator, new HttpUtil().getHttpClient());
            if (dorisConfig.getEnable2PC()) {
                dorisStreamLoad.abortPreCommit(labelPrefix, lastCheckpointId + 1);
            }
        } catch (Exception e) {
            throw new DorisConnectorException(DorisConnectorErrorCode.STREAM_LOAD_FAILED, e);
        }
        // get main work thread.
        executorThread = Thread.currentThread();
        dorisStreamLoad.startLoad(labelGenerator.generateLabel(lastCheckpointId + 1));
        // when uploading data in streaming mode, we need to regularly detect whether there are
        // exceptions.
        scheduledExecutorService.scheduleWithFixedDelay(
                this::checkDone, INITIAL_DELAY, intervalTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        checkLoadException();
        byte[] serialize = serializer.serialize(element);
        if (Objects.isNull(serialize)) {
            return;
        }
        dorisStreamLoad.writeRecord(serialize);
    }

    @Override
    public Optional<DorisCommitInfo> prepareCommit() throws IOException {
        // disable exception checker before stop load.
        loading = false;
        checkState(dorisStreamLoad != null);
        RespContent respContent = dorisStreamLoad.stopLoad();
        if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
            String errMsg =
                    String.format(
                            "stream load error: %s, see more in %s",
                            respContent.getMessage(), respContent.getErrorURL());
            throw new DorisConnectorException(DorisConnectorErrorCode.STREAM_LOAD_FAILED, errMsg);
        }
        if (!dorisConfig.getEnable2PC()) {
            return Optional.empty();
        }
        long txnId = respContent.getTxnId();

        return Optional.of(
                new DorisCommitInfo(dorisStreamLoad.getHostPort(), dorisStreamLoad.getDb(), txnId));
    }

    @Override
    public List<DorisSinkState> snapshotState(long checkpointId) throws IOException {
        checkState(dorisStreamLoad != null);
        this.dorisStreamLoad.setHostPort(getAvailableBackend());
        this.dorisStreamLoad.startLoad(labelGenerator.generateLabel(checkpointId + 1));
        this.loading = true;
        this.lastCheckpointId = checkpointId;
        return Collections.singletonList(new DorisSinkState(labelPrefix, lastCheckpointId));
    }

    @Override
    public void abortPrepare() {
        if (dorisConfig.getEnable2PC()) {
            try {
                dorisStreamLoad.abortPreCommit(labelPrefix, lastCheckpointId + 1);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void checkDone() {
        // the load future is done and checked in prepareCommit().
        // this will check error while loading.
        log.debug("start timer checker, interval {} ms", intervalTime);
        if (dorisStreamLoad.getPendingLoadFuture() != null
                && dorisStreamLoad.getPendingLoadFuture().isDone()) {
            if (!loading) {
                log.debug("not loading, skip timer checker");
                return;
            }
            String errorMsg;
            try {
                RespContent content =
                        dorisStreamLoad.handlePreCommitResponse(
                                dorisStreamLoad.getPendingLoadFuture().get());
                errorMsg = content.getMessage();
            } catch (Exception e) {
                errorMsg = e.getMessage();
            }

            loadException =
                    new DorisConnectorException(
                            DorisConnectorErrorCode.STREAM_LOAD_FAILED, errorMsg);
            log.error("stream load finished unexpectedly, interrupt worker thread! {}", errorMsg);
            // set the executor thread interrupted in case blocking in write data.
            executorThread.interrupt();
        }
    }

    private void checkLoadException() {
        if (loadException != null) {
            throw new RuntimeException("error while loading data.", loadException);
        }
    }

    @VisibleForTesting
    public boolean isLoading() {
        return this.loading;
    }

    @VisibleForTesting
    public void setDorisStreamLoad(DorisStreamLoad streamLoad) {
        this.dorisStreamLoad = streamLoad;
    }

    @VisibleForTesting
    public void setBackends(List<BackendV2.BackendRowV2> backends) {
        this.backends = backends;
    }

    @Override
    public void close() throws IOException {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        if (dorisStreamLoad != null) {
            dorisStreamLoad.close();
        }
    }

    @VisibleForTesting
    public String getAvailableBackend() {
        long tmp = pos + backends.size();
        while (pos < tmp) {
            BackendV2.BackendRowV2 backend = backends.get((int) (pos % backends.size()));
            String res = backend.toBackendString();
            if (tryHttpConnection(res)) {
                pos++;
                return res;
            }
        }
        String errMsg = "no available backend.";
        throw new DorisConnectorException(DorisConnectorErrorCode.STREAM_LOAD_FAILED, errMsg);
    }

    public boolean tryHttpConnection(String backend) {
        try {
            backend = "http://" + backend;
            URL url = new URL(backend);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(CONNECT_TIMEOUT);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception ex) {
            log.warn("Failed to connect to backend:{}", backend, ex);
            pos++;
            return false;
        }
    }

    private DorisSerializer createSerializer(
            DorisConfig dorisConfig, SeaTunnelRowType seaTunnelRowType) {
        return new SeaTunnelRowSerializer(
                dorisConfig
                        .getStreamLoadProps()
                        .getProperty(LoadConstants.FORMAT_KEY)
                        .toLowerCase(),
                seaTunnelRowType,
                dorisConfig.getStreamLoadProps().getProperty(LoadConstants.FIELD_DELIMITER_KEY),
                dorisConfig.getEnableDelete());
    }
}
