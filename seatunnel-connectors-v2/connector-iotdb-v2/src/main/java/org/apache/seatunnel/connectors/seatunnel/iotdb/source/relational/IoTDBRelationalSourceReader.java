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

package org.apache.seatunnel.connectors.seatunnel.iotdb.source.relational;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iotdb.constant.SourceConstants;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdb.serialize.DefaultSeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.iotdb.source.IoTDBAbstractSourceReader;
import org.apache.seatunnel.connectors.seatunnel.iotdb.source.IoTDBSourceSplit;

import shaded.org.apache.iotdb.isession.ITableSession;
import shaded.org.apache.iotdb.isession.SessionDataSet;
import shaded.org.apache.iotdb.isession.util.Version;
import shaded.org.apache.iotdb.rpc.IoTDBConnectionException;
import shaded.org.apache.iotdb.session.TableSessionBuilder;
import shaded.org.apache.tsfile.read.common.RowRecord;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.FETCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.NODE_URLS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.THRIFT_DEFAULT_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.THRIFT_MAX_FRAME_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions.VERSION;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.constant.SourceConstants.NODES_SPLIT;

public class IoTDBRelationalSourceReader extends IoTDBAbstractSourceReader {

    private ITableSession tableSession;

    public IoTDBRelationalSourceReader(
            ReadonlyConfig conf, SourceReader.Context readerContext, SeaTunnelRowType rowType) {
        super(conf, readerContext);
        this.deserializer = new DefaultSeaTunnelRowDeserializer(rowType, SourceConstants.TABLE);
    }

    @Override
    public void open() throws Exception {
        tableSession = buildTableSession(conf);
    }

    @Override
    public void close() throws IOException {
        try {
            if (tableSession != null) {
                tableSession.close();
            }
        } catch (IoTDBConnectionException e) {
            throw new IotdbConnectorException(
                    IotdbConnectorErrorCode.CLOSE_SESSION_FAILED, "Close IoTDB session failed", e);
        }
    }

    private ITableSession buildTableSession(ReadonlyConfig conf) throws IoTDBConnectionException {
        TableSessionBuilder sessionBuilder = new TableSessionBuilder().enableCompression(false);
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
        if (null != conf.get(DATABASE)) {
            sessionBuilder.database(conf.get(DATABASE));
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
            sessionBuilder.version = Version.valueOf(conf.get(VERSION));
        }

        return sessionBuilder.build();
    }

    @Override
    public void read(IoTDBSourceSplit split, Collector<SeaTunnelRow> output) throws Exception {
        try (SessionDataSet dataSet =
                tableSession.executeQueryStatement(split.getQuery(), Long.MAX_VALUE)) {
            while (dataSet.hasNext()) {
                RowRecord rowRecord = dataSet.next();
                SeaTunnelRow seaTunnelRow = deserializer.deserialize(rowRecord);
                output.collect(seaTunnelRow);
            }
        }
    }
}
