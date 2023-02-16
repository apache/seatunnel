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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connector.selectdb.config.SelectDBConfig;
import org.apache.seatunnel.connector.selectdb.exception.SelectDBConnectorException;
import org.apache.seatunnel.connector.selectdb.serialize.SelectDBCsvSerializer;
import org.apache.seatunnel.connector.selectdb.serialize.SelectDBJsonSerializer;
import org.apache.seatunnel.connector.selectdb.serialize.SelectDBSerializer;
import org.apache.seatunnel.connector.selectdb.sink.committer.SelectDBCommitInfo;
import org.apache.seatunnel.connector.selectdb.util.HttpUtil;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.seatunnel.connector.selectdb.exception.SelectDBConnectorErrorCode.WHILE_LOADING_FAILED;
import static org.apache.seatunnel.connector.selectdb.sink.writer.LoadConstants.FIELD_DELIMITER_KEY;
import static org.apache.seatunnel.connector.selectdb.sink.writer.LoadConstants.FORMAT_KEY;
import static org.apache.seatunnel.connector.selectdb.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.seatunnel.connector.selectdb.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

@Slf4j
public class SelectDBSinkWriter
        implements SinkWriter<SeaTunnelRow, SelectDBCommitInfo, SelectDBSinkState> {
    private final SelectDBConfig selectdbConfig;
    private final long lastCheckpointId;
    private volatile long currentCheckpointId;
    private SelectDBCopyInto selectdbCopyInto;
    volatile boolean loading;
    private final String labelPrefix;
    private final byte[] lineDelimiter;
    private final LabelGenerator labelGenerator;
    private final int intervalTime;
    private final SelectDBSinkState selectdbSinkState;
    private final SelectDBSerializer serializer;
    private final transient ScheduledExecutorService scheduledExecutorService;
    private transient volatile Exception loadException = null;
    private final AtomicInteger fileNum;

    private final ArrayList<byte[]> cache = new ArrayList<>();
    private int cacheSize = 0;
    private int cacheCnt = 0;

    private static final long MAX_CACHE_SIZE = 1024 * 1024L;
    private static final int INITIAL_DELAY = 1000;

    public SelectDBSinkWriter(
            SinkWriter.Context context,
            List<SelectDBSinkState> state,
            SeaTunnelRowType seaTunnelRowType,
            Config pluginConfig) {
        this.selectdbConfig = SelectDBConfig.loadConfig(pluginConfig);
        this.lastCheckpointId = context.getIndexOfSubtask();
        log.info("restore checkpointId {}", lastCheckpointId);
        this.currentCheckpointId = lastCheckpointId;
        log.info("labelPrefix " + selectdbConfig.getLabelPrefix());
        this.selectdbSinkState = new SelectDBSinkState(selectdbConfig.getLabelPrefix());
        this.labelPrefix = selectdbConfig.getLabelPrefix() + "_" + context.getIndexOfSubtask();
        this.lineDelimiter =
                selectdbConfig
                        .getStreamLoadProps()
                        .getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT)
                        .getBytes();
        this.labelGenerator = new LabelGenerator(labelPrefix, selectdbConfig.getEnable2PC());
        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                                .setNameFormat("file-load-check-" + context.getIndexOfSubtask())
                                .build());
        this.serializer = createSerializer(selectdbConfig, seaTunnelRowType);
        this.intervalTime = selectdbConfig.getCheckInterval();
        this.loading = false;
        this.fileNum = new AtomicInteger();
    }

    public void initializeLoad(List<SelectDBSinkState> state) throws IOException {
        this.selectdbCopyInto =
                new SelectDBCopyInto(
                        selectdbConfig, labelGenerator, new HttpUtil().getHttpClient());
        currentCheckpointId = lastCheckpointId + 1;
        scheduledExecutorService.scheduleWithFixedDelay(
                this::checkDone, INITIAL_DELAY, intervalTime, TimeUnit.MILLISECONDS);
        serializer.open();
    }

    @Override
    public synchronized void write(SeaTunnelRow element) throws IOException {
        checkLoadException();
        byte[] serialize = serializer.serialize(element);
        if (Objects.isNull(serialize)) {
            // schema change is null
            return;
        }
        if (cacheSize > MAX_CACHE_SIZE) {
            flush(serialize);
        } else {
            cacheSize += serialize.length;
            cacheCnt++;
            cache.add(serialize);
        }
    }

    public synchronized void flush(byte[] serialize) throws IOException {
        if (!loading) {
            log.info("start load by cache full, cnt {}, size {}", cacheCnt, cacheSize);
            startLoad();
        }
        this.selectdbCopyInto.writeRecord(serialize);
    }

    @Override
    public synchronized Optional<SelectDBCommitInfo> prepareCommit() throws IOException {
        checkState(selectdbCopyInto != null);
        if (!loading) {
            // No data was written during the entire checkpoint period
            log.info("start load by checkpoint, cnt {} size {} ", cacheCnt, cacheSize);
            startLoad();
        }
        log.info("stop load by checkpoint");
        stopLoad();
        CopySQLBuilder copySQLBuilder =
                new CopySQLBuilder(selectdbConfig, selectdbCopyInto.getFileList());
        String copySql = copySQLBuilder.buildCopySQL();
        return Optional.of(
                new SelectDBCommitInfo(
                        selectdbCopyInto.getHostPort(), selectdbConfig.getClusterName(), copySql));
    }

    @Override
    public synchronized List<SelectDBSinkState> snapshotState(long checkpointId)
            throws IOException {
        checkState(selectdbCopyInto != null);
        this.currentCheckpointId = checkpointId + 1;

        log.info("clear the file list {}", selectdbCopyInto.getFileList());
        this.fileNum.set(0);
        this.selectdbCopyInto.clearFileList();
        return Collections.singletonList(selectdbSinkState);
    }

    @Override
    public void abortPrepare() {}

    private synchronized void startLoad() throws IOException {
        // If not started writing, make a streaming request
        this.selectdbCopyInto.startLoad(
                labelGenerator.generateLabel(currentCheckpointId, fileNum.getAndIncrement()));
        if (!cache.isEmpty()) {
            // add line delimiter
            ByteBuffer buf =
                    ByteBuffer.allocate(cacheSize + (cache.size() - 1) * lineDelimiter.length);
            for (int i = 0; i < cache.size(); i++) {
                if (i > 0) {
                    buf.put(lineDelimiter);
                }
                buf.put(cache.get(i));
            }
            this.selectdbCopyInto.writeRecord(buf.array());
        }
        this.loading = true;
    }

    private synchronized void stopLoad() throws IOException {
        this.loading = false;
        this.selectdbCopyInto.stopLoad();
        this.cacheSize = 0;
        this.cacheCnt = 0;
        this.cache.clear();
    }

    private synchronized void checkDone() {
        // s3 can't keep http long links, generate data files regularly
        log.info("start timer checker, interval {} ms", intervalTime);
        try {
            if (!loading) {
                log.info("not loading, skip timer checker");
                return;
            }
            if (selectdbCopyInto.getPendingLoadFuture() != null
                    && !selectdbCopyInto.getPendingLoadFuture().isDone()) {
                log.info("stop load by timer checker");
                stopLoad();
            }
        } catch (Exception ex) {
            log.error("upload file failed, thread exited:", ex);
            loadException = ex;
        }
    }

    private void checkLoadException() {
        if (loadException != null) {
            throw new SelectDBConnectorException(WHILE_LOADING_FAILED, loadException);
        }
    }

    @Override
    public void close() throws IOException {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        if (selectdbCopyInto != null) {
            selectdbCopyInto.close();
        }
        serializer.close();
    }

    public static SelectDBSerializer createSerializer(
            SelectDBConfig selectdbConfig, SeaTunnelRowType seaTunnelRowType) {
        if (LoadConstants.CSV.equals(selectdbConfig.getStreamLoadProps().getProperty(FORMAT_KEY))) {
            return new SelectDBCsvSerializer(
                    selectdbConfig.getStreamLoadProps().getProperty(FIELD_DELIMITER_KEY),
                    seaTunnelRowType);
        }
        if (LoadConstants.JSON.equals(
                selectdbConfig.getStreamLoadProps().getProperty(FORMAT_KEY))) {
            return new SelectDBJsonSerializer(seaTunnelRowType);
        }
        throw new SelectDBConnectorException(
                CommonErrorCode.ILLEGAL_ARGUMENT,
                "Failed to create row serializer, unsupported `format` from copy into load properties.");
    }
}
