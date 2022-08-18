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

import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.util.Version;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class IoTDBSourceReader implements SourceReader<SeaTunnelRow, IoTDBSourceSplit> {

    private static final long THREAD_WAIT_TIME = 500L;

    private Map<String, Object> conf;

    private Set<IoTDBSourceSplit> sourceSplits;

    private final SourceReader.Context context;

    private SeaTunnelRowType seaTunnelRowType;

    private Session session;

    public IoTDBSourceReader(Map<String, Object> conf, SourceReader.Context readerContext, SeaTunnelRowType seaTunnelRowType) {
        this.conf = conf;
        this.sourceSplits = new HashSet<>();
        this.context = readerContext;
        this.seaTunnelRowType = seaTunnelRowType;
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
            session.close();
        } catch (IoTDBConnectionException e) {
            throw new IOException("close IoTDB session failed", e);
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (sourceSplits.isEmpty()) {
            Thread.sleep(THREAD_WAIT_TIME);
            return;
        }
        sourceSplits.forEach(source -> {
            try {
                read(source, output);
            } catch (Exception e) {
                throw new RuntimeException("IotDB source read error", e);
            }
        });

        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded fake source");
            context.signalNoMoreElement();
        }
    }

    private void read(IoTDBSourceSplit split, Collector<SeaTunnelRow> output) throws Exception {
        try (SessionDataSet dataSet = session.executeQueryStatement(split.getQuery())) {
            while (dataSet.hasNext()) {
                RowRecord row = dataSet.next();
                Object[] datas = new Object[row.getFields().size()];
                for (int i = 0; i < row.getFields().size(); i++) {
                    row.getFields().get(i).getDataType();
                    datas[i] = convertToDataType(row.getFields().get(i));
                }
                output.collect(new SeaTunnelRow(datas));
            }
        }
    }

    private Object convertToDataType(Field field) {

        switch (field.getDataType()) {
            case INT32:
                return field.getIntV();
            case INT64:
                return field.getLongV();
            case FLOAT:
                return field.getFloatV();
            case DOUBLE:
                return field.getDoubleV();
            case TEXT:
                return field.getStringValue();
            case BOOLEAN:
                return field.getBoolV();
            default:
                throw new IllegalArgumentException("unknown TSData type: " + field.getDataType());
        }
    }

    private Session buildSession(Map<String, Object> conf) {
        Session.Builder sessionBuilder = new Session.Builder();
        if (conf.containsKey(HOST)) {
            sessionBuilder
                    .host((String) conf.get(HOST))
                    .port(Integer.parseInt(conf.get(PORT).toString()))
                    .build();
        } else {
            String nodeUrlsString = (String) conf.get(NODE_URLS);

            List<String> nodes = Stream.of(nodeUrlsString.split(NODES_SPLIT)).collect(Collectors.toList());
            sessionBuilder.nodeUrls(nodes);
        }
        if (null != conf.get(FETCH_SIZE)) {
            sessionBuilder.fetchSize(Integer.parseInt(conf.get(FETCH_SIZE).toString()));
        }
        if (null != conf.get(USERNAME)) {
            sessionBuilder.username((String) conf.get(USERNAME));
        }
        if (null != conf.get(PASSWORD)) {
            sessionBuilder.password((String) conf.get(PASSWORD));
        }
        if (null != conf.get(THRIFT_DEFAULT_BUFFER_SIZE)) {
            sessionBuilder.thriftDefaultBufferSize(Integer.parseInt(conf.get(THRIFT_DEFAULT_BUFFER_SIZE).toString()));
        }
        if (null != conf.get(THRIFT_MAX_FRAME_SIZE)) {
            sessionBuilder.thriftMaxFrameSize(Integer.parseInt(conf.get(THRIFT_MAX_FRAME_SIZE).toString()));
        }
        if (null != conf.get(ENABLE_CACHE_LEADER)) {
            sessionBuilder.enableCacheLeader(Boolean.parseBoolean(conf.get(ENABLE_CACHE_LEADER).toString()));
        }
        if (null != conf.get(VERSION)) {
            Version version = Version.valueOf(conf.get(VERSION).toString());
            sessionBuilder.version(version);
        }
        return sessionBuilder.build();
    }

    @Override
    public List<IoTDBSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void addSplits(List<IoTDBSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        // do nothing
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }
}
