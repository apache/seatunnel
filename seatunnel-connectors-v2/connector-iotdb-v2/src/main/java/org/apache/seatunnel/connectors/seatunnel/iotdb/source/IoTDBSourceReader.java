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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iotdb.constant.SourceConstants;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdb.serialize.DefaultSeaTunnelRowDeserializer;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.read.common.RowRecord;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.ENABLE_CACHE_LEADER;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.FETCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.NODE_URLS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.THRIFT_DEFAULT_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.THRIFT_MAX_FRAME_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.VERSION;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.constant.SourceConstants.NODES_SPLIT;

public class IoTDBSourceReader extends IoTDBAbstractSourceReader {

    private Session session;

    public IoTDBSourceReader(
            ReadonlyConfig conf, SourceReader.Context readerContext, SeaTunnelRowType rowType) {
        super(conf, readerContext);
        this.deserializer = new DefaultSeaTunnelRowDeserializer(rowType, SourceConstants.TREE);
    }

    @Override
    public void open() throws Exception {
        session = buildSession(conf);
        session.open();
    }

    @Override
    public void close() throws IOException {
        try {
            if (session != null) {
                session.close();
            }
        } catch (IoTDBConnectionException e) {
            throw new IotdbConnectorException(
                    IotdbConnectorErrorCode.CLOSE_SESSION_FAILED, "Close IoTDB session failed", e);
        }
    }

    private Session buildSession(ReadonlyConfig conf) {
        Session.Builder sessionBuilder = new Session.Builder();
        String nodeUrlsString = conf.get(NODE_URLS);
        List<String> nodes =
                Stream.of(nodeUrlsString.split(NODES_SPLIT)).collect(Collectors.toList());
        sessionBuilder.nodeUrls(nodes);
        if (null != conf.get(FETCH_SIZE)) {
            sessionBuilder.fetchSize(Integer.parseInt(conf.get(FETCH_SIZE).toString()));
        }
        if (null != conf.get(USERNAME)) {
            sessionBuilder.username(conf.get(USERNAME));
        }
        if (null != conf.get(PASSWORD)) {
            sessionBuilder.password(conf.get(PASSWORD));
        }
        if (null != conf.get(THRIFT_DEFAULT_BUFFER_SIZE)) {
            sessionBuilder.thriftDefaultBufferSize(
                    Integer.parseInt(conf.get(THRIFT_DEFAULT_BUFFER_SIZE).toString()));
        }
        if (null != conf.get(THRIFT_MAX_FRAME_SIZE)) {
            sessionBuilder.thriftMaxFrameSize(
                    Integer.parseInt(conf.get(THRIFT_MAX_FRAME_SIZE).toString()));
        }
        if (null != conf.get(VERSION)) {
            Version version = Version.valueOf(conf.get(VERSION));
            sessionBuilder.version(version);
        }
        Session session = sessionBuilder.build();
        if (null != conf.get(ENABLE_CACHE_LEADER)) {
            session.setEnableCacheLeader(
                    Boolean.parseBoolean(conf.get(ENABLE_CACHE_LEADER).toString()));
        }
        return session;
    }

    @Override
    public void read(IoTDBSourceSplit split, Collector<SeaTunnelRow> output) throws Exception {
        try (SessionDataSet dataSet = session.executeQueryStatement(split.getQuery())) {
            while (dataSet.hasNext()) {
                RowRecord rowRecord = dataSet.next();
                SeaTunnelRow seaTunnelRow = deserializer.deserialize(rowRecord);
                output.collect(seaTunnelRow);
            }
        }
    }
}
