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

package org.apache.seatunnel.connectors.seatunnel.iotdb.source;

import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.ENABLE_CACHE_LEADER;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.FETCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.NODE_URLS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.PORT;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.THRIFT_DEFAULT_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.THRIFT_MAX_FRAME_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.VERSION;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.constant.SourceConstants.NODES_SPLIT;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdb.serialize.DefaultSeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.iotdb.serialize.SeaTunnelRowDeserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.util.Version;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class IoTDBSourceReader implements SourceReader<SeaTunnelRow, IoTDBSourceSplit> {

    private final Map<String, Object> conf;

    private final Queue<IoTDBSourceSplit> pendingSplits;

    private final SourceReader.Context context;

    private final SeaTunnelRowDeserializer deserializer;

    private Session session;

    private volatile boolean noMoreSplitsAssignment;

    public IoTDBSourceReader(Map<String, Object> conf,
                             SourceReader.Context readerContext,
                             SeaTunnelRowType rowType) {
        this.conf = conf;
        this.pendingSplits = new LinkedList<>();
        this.context = readerContext;
        this.deserializer = new DefaultSeaTunnelRowDeserializer(rowType);
    }

    @Override
    public void open() throws IoTDBConnectionException {
        session = buildSession(conf);
        session.open();
    }

    @Override
    public void close() throws IOException {
        //nothing to do
        try {
            if (session != null) {
                session.close();
            }
        } catch (IoTDBConnectionException e) {
            throw new IotdbConnectorException(IotdbConnectorErrorCode.CLOSE_SESSION_FAILED,
                "Close IoTDB session failed", e);
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        while (!pendingSplits.isEmpty()) {
            synchronized (output.getCheckpointLock()) {
                IoTDBSourceSplit split = pendingSplits.poll();
                read(split, output);
            }
        }

        if (Boundedness.BOUNDED.equals(context.getBoundedness())
            && noMoreSplitsAssignment
            && pendingSplits.isEmpty()) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded iotdb source");
            context.signalNoMoreElement();
        }
    }

    private void read(IoTDBSourceSplit split, Collector<SeaTunnelRow> output) throws Exception {
        try (SessionDataSet dataSet = session.executeQueryStatement(split.getQuery())) {
            while (dataSet.hasNext()) {
                RowRecord rowRecord = dataSet.next();
                SeaTunnelRow seaTunnelRow = deserializer.deserialize(rowRecord);
                output.collect(seaTunnelRow);
            }
        }
    }

    private Session buildSession(Map<String, Object> conf) {
        Session.Builder sessionBuilder = new Session.Builder();
        if (conf.containsKey(HOST.key())) {
            sessionBuilder
                .host((String) conf.get(HOST.key()))
                .port(Integer.parseInt(conf.get(PORT.key()).toString()))
                .build();
        } else {
            String nodeUrlsString = (String) conf.get(NODE_URLS.key());
            List<String> nodes = Stream.of(nodeUrlsString.split(NODES_SPLIT)).collect(Collectors.toList());
            sessionBuilder.nodeUrls(nodes);
        }
        if (null != conf.get(FETCH_SIZE.key())) {
            sessionBuilder.fetchSize(Integer.parseInt(conf.get(FETCH_SIZE.key()).toString()));
        }
        if (null != conf.get(USERNAME.key())) {
            sessionBuilder.username((String) conf.get(USERNAME.key()));
        }
        if (null != conf.get(PASSWORD.key())) {
            sessionBuilder.password((String) conf.get(PASSWORD.key()));
        }
        if (null != conf.get(THRIFT_DEFAULT_BUFFER_SIZE.key())) {
            sessionBuilder.thriftDefaultBufferSize(Integer.parseInt(conf.get(THRIFT_DEFAULT_BUFFER_SIZE.key()).toString()));
        }
        if (null != conf.get(THRIFT_MAX_FRAME_SIZE.key())) {
            sessionBuilder.thriftMaxFrameSize(Integer.parseInt(conf.get(THRIFT_MAX_FRAME_SIZE.key()).toString()));
        }
        if (null != conf.get(ENABLE_CACHE_LEADER.key())) {
            sessionBuilder.enableCacheLeader(Boolean.parseBoolean(conf.get(ENABLE_CACHE_LEADER.key()).toString()));
        }
        if (null != conf.get(VERSION.key())) {
            Version version = Version.valueOf(conf.get(VERSION.key()).toString());
            sessionBuilder.version(version);
        }
        return sessionBuilder.build();
    }

    @Override
    public List<IoTDBSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<IoTDBSourceSplit> splits) {
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader received NoMoreSplits event.");
        noMoreSplitsAssignment = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }
}
