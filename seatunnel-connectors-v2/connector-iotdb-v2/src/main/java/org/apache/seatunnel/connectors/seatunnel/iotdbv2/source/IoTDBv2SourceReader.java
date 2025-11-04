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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.constant.SourceConstants;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.exception.IotdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.DefaultSeaTunnelRowDeserializer;

import shaded.org.apache.iotdb.isession.SessionDataSet;
import shaded.org.apache.iotdb.rpc.IoTDBConnectionException;
import shaded.org.apache.iotdb.session.Session;
import shaded.org.apache.tsfile.read.common.RowRecord;

import java.io.IOException;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.IoTDBv2SourceOptions.DEFAULT_THRIFT_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.IoTDBv2SourceOptions.ENABLE_CACHE_LEADER;
import static org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.IoTDBv2SourceOptions.FETCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.IoTDBv2SourceOptions.MAX_THRIFT_FRAME_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.IoTDBv2SourceOptions.NODE_URLS;
import static org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.IoTDBv2SourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.IoTDBv2SourceOptions.USERNAME;

public class IoTDBv2SourceReader extends IoTDBv2AbstractSourceReader {

    private Session session;

    public IoTDBv2SourceReader(
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
        List<String> nodes = conf.get(NODE_URLS);
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
        if (null != conf.get(DEFAULT_THRIFT_BUFFER_SIZE)) {
            sessionBuilder.thriftDefaultBufferSize(
                    Integer.parseInt(conf.get(DEFAULT_THRIFT_BUFFER_SIZE).toString()));
        }
        if (null != conf.get(MAX_THRIFT_FRAME_SIZE)) {
            sessionBuilder.thriftMaxFrameSize(
                    Integer.parseInt(conf.get(MAX_THRIFT_FRAME_SIZE).toString()));
        }
        Session session = sessionBuilder.build();
        if (null != conf.get(ENABLE_CACHE_LEADER)) {
            session.setEnableCacheLeader(
                    Boolean.parseBoolean(conf.get(ENABLE_CACHE_LEADER).toString()));
        }
        return session;
    }

    @Override
    public void read(IoTDBv2SourceSplit split, Collector<SeaTunnelRow> output) throws Exception {
        try (SessionDataSet dataSet = session.executeQueryStatement(split.getQuery())) {
            while (dataSet.hasNext()) {
                RowRecord rowRecord = dataSet.next();
                SeaTunnelRow seaTunnelRow = deserializer.deserialize(rowRecord);
                output.collect(seaTunnelRow);
            }
        }
    }
}
