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

package org.apache.seatunnel.connectors.seatunnel.inlong.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.inlong.config.InlongSemantics;
import org.apache.seatunnel.connectors.seatunnel.inlong.exception.InlongErrorCode;
import org.apache.seatunnel.connectors.seatunnel.inlong.exception.InlongException;
import org.apache.seatunnel.connectors.seatunnel.inlong.state.InlongCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.inlong.state.InlongSinkState;
import org.apache.seatunnel.connectors.seatunnel.inlong.util.InlongUtil;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.exception.ProxySdkException;
import org.apache.inlong.sdk.dataproxy.sender.MsgSendCallback;
import org.apache.inlong.sdk.dataproxy.sender.tcp.InLongTcpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpEventInfo;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.ACTIVE_CONNECT_NUM;
import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.ASYNC_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.BATCH_SEND_LEN;
import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.ENABLE_AUTH;
import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.GROUP_ID;
import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.MANAGER_URL;
import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.SECRET_ID;
import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.SECRET_KEY;
import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.SEMANTICS;
import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.STREAM_ID;
import static org.apache.seatunnel.connectors.seatunnel.inlong.config.SinkProperties.THREAD_NUM;

class InlongSinkWriter implements SinkWriter<SeaTunnelRow, InlongCommitInfo, InlongSinkState> {

    public static final int BATCH_SEND_INTERVAL = 1;
    public static final int MAX_RETRY = 5;
    public static final int ERROR_LOG_SAMPLE = 10;
    public static final int RESEND_QUEUE_WAIT_MS = 10;
    private final long WRITE_INTERVAL_MS = 10;
    private final long CACHE_LIMIT = 100 * 1024 * 1024;
    private final long THREAD_ALIVE_TIME_MS = 60 * 1000;
    private InLongTcpMsgSender sender;
    private InlongSemantics inlongSemantics;
    private final String groupId;
    private final String streamId;
    private LinkedBlockingQueue<byte[]> rowQue;
    private LinkedBlockingQueue<SenderCallback> resendQue;
    private AtomicLong cacheLen = new AtomicLong(0);
    private final int batchSendLen;
    private static ThreadPoolExecutor EXECUTOR_SERVICE =
            new ThreadPoolExecutor(
                    0,
                    Integer.MAX_VALUE,
                    1L,
                    TimeUnit.SECONDS,
                    new SynchronousQueue<>(),
                    new InlongThreadFactory("inlong-sink"));
    private volatile boolean shutdown = false;
    private static final Logger LOG = LoggerFactory.getLogger(InlongSinkWriter.class);
    private final SerializationSchema schema;
    private volatile long sendThreadHeartbeatTime;
    private volatile long resendThreadHeartbeatTime;

    public InlongSinkWriter(
            Context context,
            ReadonlyConfig inlongConfig,
            SeaTunnelRowType seaTunnelRowType,
            List<InlongSinkState> pulsarStates) {
        inlongSemantics = inlongConfig.get(SEMANTICS);
        TcpMsgSenderConfig proxyClientConfig = null;
        rowQue = new LinkedBlockingQueue<>();
        resendQue = new LinkedBlockingQueue<>();
        batchSendLen = inlongConfig.get(BATCH_SEND_LEN);
        groupId = inlongConfig.get(GROUP_ID);
        streamId = inlongConfig.get(STREAM_ID);
        schema =
                TextSerializationSchema.builder()
                        .seaTunnelRowType(seaTunnelRowType)
                        .delimiter(inlongConfig.get(FIELD_DELIMITER))
                        .build();
        try {
            if (inlongConfig.get(ENABLE_AUTH)) {
                proxyClientConfig =
                        new TcpMsgSenderConfig(
                                inlongConfig.get(MANAGER_URL),
                                groupId,
                                inlongConfig.get(SECRET_ID),
                                inlongConfig.get(SECRET_KEY));
            } else {
                proxyClientConfig = new TcpMsgSenderConfig(inlongConfig.get(MANAGER_URL), groupId);
            }
        } catch (ProxySdkException e) {
            LOG.error("Failed to create TcpMsgSenderConfig", e);
            throw new RuntimeException(e);
        }
        proxyClientConfig.setMaxInFlightSizeInKb(inlongConfig.get(ASYNC_BUFFER_SIZE));
        proxyClientConfig.setAliveConnections(inlongConfig.get(ACTIVE_CONNECT_NUM));
        proxyClientConfig.setRequestTimeoutMs(inlongConfig.get(REQUEST_TIMEOUT));
        proxyClientConfig.setNettyWorkerThreadNum(inlongConfig.get(THREAD_NUM));
        sender =
                new InLongTcpMsgSender(
                        proxyClientConfig,
                        new DefaultThreadFactory(
                                "inlong-sender", Thread.currentThread().isDaemon()));
        ProcessResult procResult = new ProcessResult();
        if (!sender.start(procResult)) {
            LOG.error("Failed to start InLongTcpMsgSender");
            sender.close();
        }
        sendThreadHeartbeatTime = System.currentTimeMillis();
        resendThreadHeartbeatTime = System.currentTimeMillis();
        EXECUTOR_SERVICE.submit(coreThread());
        EXECUTOR_SERVICE.submit(resend());
    }

    @Override
    public void write(SeaTunnelRow element) {
        checkSinkState();
        while (!shutdown && !resendQue.isEmpty()) {
            InlongUtil.silenceSleepInMs(WRITE_INTERVAL_MS);
        }
        while (cacheLen.get() > CACHE_LIMIT) {
            InlongUtil.silenceSleepInMs(WRITE_INTERVAL_MS);
        }
        byte[] data = schema.serialize(element);
        while (!shutdown && !rowQue.offer(data)) {
            InlongUtil.silenceSleepInMs(WRITE_INTERVAL_MS);
        }
        cacheLen.addAndGet(data.length);
    }

    private void checkSinkState() throws InlongException {
        if (System.currentTimeMillis() - sendThreadHeartbeatTime > THREAD_ALIVE_TIME_MS) {
            throw new InlongException(
                    InlongErrorCode.SENDING_THREAD_NOT_RUNNING,
                    "Sending thread heartbeat timeout exceeded");
        }
        if (System.currentTimeMillis() - resendThreadHeartbeatTime > THREAD_ALIVE_TIME_MS) {
            throw new InlongException(
                    InlongErrorCode.RESEND_THREAD_NOT_RUNNING,
                    "Resend thread heartbeat timeout exceeded");
        }
    }

    private Runnable coreThread() {
        return () -> {
            LOG.info("start send {}:{}", groupId, streamId);
            while (!shutdown) {
                try {
                    SenderMessage msg = fetchSenderMessage();
                    sendThreadHeartbeatTime = System.currentTimeMillis();
                    if (msg != null) {
                        sendBatchWithRetryCount(msg, 0);
                    }
                } catch (Throwable e) {
                    LOG.error("send message failed: ", e);
                }
                InlongUtil.silenceSleepInMs(BATCH_SEND_INTERVAL);
            }
            LOG.info("start send {}:{}", groupId, streamId);
        };
    }

    private Runnable resend() {
        return () -> {
            LOG.info("start resend {}:{}", groupId, streamId);
            while (!shutdown) {
                try {
                    SenderCallback callback =
                            resendQue.poll(RESEND_QUEUE_WAIT_MS, TimeUnit.MILLISECONDS);
                    resendThreadHeartbeatTime = System.currentTimeMillis();
                    if (callback != null) {
                        sendBatchWithRetryCount(callback.message, callback.retry + 1);
                    }
                } catch (Throwable e) {
                    LOG.error("resend message failed: ", e);
                } finally {
                    InlongUtil.silenceSleepInMs(BATCH_SEND_INTERVAL);
                }
            }
            LOG.info("stop resend {}:{}", groupId, streamId);
        };
    }

    private void sendBatchWithRetryCount(SenderMessage message, int retry) {
        boolean suc = false;
        while (!suc && !shutdown) {
            try {
                SenderCallback cb = new SenderCallback(message, retry);
                ProcessResult procResult = new ProcessResult();
                boolean isSuccess =
                        sender.asyncSendMessage(
                                new TcpEventInfo(
                                        groupId,
                                        streamId,
                                        message.getDataTime(),
                                        0,
                                        message.getExtraMap(),
                                        message.getDataList()),
                                cb,
                                procResult);
                if (!isSuccess) {
                    throw new ProxySdkException("Send message failure, " + procResult);
                }
                suc = true;
            } catch (Exception e) {
                suc = false;
                if (retry > MAX_RETRY) {
                    if (retry % ERROR_LOG_SAMPLE == 0) {
                        LOG.error("max retry reached, sample log error: ", e);
                    }
                } else {
                    LOG.error("exception caught", e);
                }
                retry++;
                InlongUtil.silenceSleepInMs(WRITE_INTERVAL_MS);
            }
        }
    }

    @Override
    public Optional<InlongCommitInfo> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public List<InlongSinkState> snapshotState(long checkpointId) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() {
        shutdown = true;
        sender.close();
    }

    public SenderMessage fetchSenderMessage() {
        int resultBatchSize = 0;
        List<byte[]> bodyList = new ArrayList<>();
        while (!rowQue.isEmpty()) {
            byte[] peekMessage = rowQue.peek();
            int peekMessageLength = peekMessage.length;
            if (resultBatchSize + peekMessageLength > batchSendLen) {
                break;
            }
            byte[] data = rowQue.remove();
            int bodySize = data.length;
            if (peekMessageLength > batchSendLen) {
                LOG.warn(
                        "message size is {}, greater than max pack size {}, drop it!",
                        peekMessage.length,
                        batchSendLen);
                rowQue.remove();
                break;
            }
            resultBatchSize += bodySize;
            bodyList.add(data);
        }
        if (!bodyList.isEmpty()) {
            Map<String, String> extraMap = new HashMap<>();
            SenderMessage senderMessage =
                    new SenderMessage(
                            groupId, streamId, bodyList, System.currentTimeMillis(), extraMap);
            return senderMessage;
        }
        return null;
    }

    /** sender callback */
    private class SenderCallback implements MsgSendCallback {

        private final int retry;
        private final SenderMessage message;
        private final int msgCnt;

        SenderCallback(SenderMessage message, int retry) {
            this.message = message;
            this.retry = retry;
            this.msgCnt = message.getDataList().size();
        }

        @Override
        public void onMessageAck(ProcessResult result) {
            if (result.isSuccess()) {
                cacheLen.addAndGet(-message.getTotalSize());
            } else {
                LOG.error(
                        "send groupId {}, streamId {}, dataTime {} fail with times {}, error {}",
                        groupId,
                        streamId,
                        message.getDataTime(),
                        retry,
                        result);
                try {
                    resendQue.put(new SenderCallback(message, retry));
                } catch (Throwable throwable) {
                    LOG.error("putInResendQueue error: ", throwable);
                }
            }
        }

        @Override
        public void onException(Throwable e) {
            LOG.error("SenderCallback error: ", e);
        }
    }
}
