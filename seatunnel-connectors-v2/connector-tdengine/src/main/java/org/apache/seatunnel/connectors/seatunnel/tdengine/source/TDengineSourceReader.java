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

package org.apache.seatunnel.connectors.seatunnel.tdengine.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.tdengine.exception.TDengineConnectorException;

import com.taosdata.jdbc.TSDBDriver;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.apache.seatunnel.connectors.seatunnel.tdengine.utils.TDengineUtil.checkDriverExist;

@Slf4j
public class TDengineSourceReader implements SourceReader<SeaTunnelRow, TDengineSourceSplit> {
    private final TDengineSourceConfig config;

    private final Deque<TDengineSourceSplit> sourceSplits;

    private final Context context;

    private Connection conn;

    private volatile boolean noMoreSplit;

    public TDengineSourceReader(TDengineSourceConfig config, SourceReader.Context readerContext) {
        this.config = config;
        this.sourceSplits = new ConcurrentLinkedDeque<>();
        this.context = readerContext;
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> collector) throws InterruptedException {
        synchronized (collector.getCheckpointLock()) {
            log.info("polling new split from queue!");
            TDengineSourceSplit split = sourceSplits.poll();
            if (Objects.nonNull(split)) {
                log.info(
                        "starting run new split {}, query sql: {}!",
                        split.splitId(),
                        split.getQuery());
                try {
                    read(split, collector);
                } catch (Exception e) {
                    throw new TDengineConnectorException(
                            CommonErrorCodeDeprecated.READER_OPERATION_FAILED,
                            "TDengine split read error",
                            e);
                }
            } else if (noMoreSplit && sourceSplits.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded TDengine source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(1000L);
            }
        }
    }

    @Override
    public void open() {
        String jdbcUrl = config.getUrl();

        Properties properties = new Properties();
        properties.put(TSDBDriver.PROPERTY_KEY_USER, config.getUsername());
        properties.put(TSDBDriver.PROPERTY_KEY_PASSWORD, config.getPassword());

        try {
            checkDriverExist(jdbcUrl);
            conn = DriverManager.getConnection(jdbcUrl, properties);
        } catch (SQLException e) {
            throw new TDengineConnectorException(
                    CommonErrorCodeDeprecated.READER_OPERATION_FAILED,
                    "get TDengine connection failed:" + jdbcUrl,
                    e);
        }
    }

    @Override
    public void close() {
        try {
            if (!Objects.isNull(conn)) {
                conn.close();
            }
        } catch (SQLException e) {
            throw new TDengineConnectorException(
                    CommonErrorCodeDeprecated.READER_OPERATION_FAILED,
                    "TDengine reader connection close failed",
                    e);
        }
    }

    private void read(TDengineSourceSplit split, Collector<SeaTunnelRow> output) throws Exception {
        try (Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(split.getQuery())) {
            ResultSetMetaData meta = resultSet.getMetaData();

            while (resultSet.next()) {
                Object[] datas = new Object[meta.getColumnCount() + 1];
                datas[0] = split.splitId();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    datas[i] = convertDataType(resultSet.getObject(i));
                }
                output.collect(new SeaTunnelRow(datas));
            }
        }
    }

    private Object convertDataType(Object object) {
        if (Objects.isNull(object)) return null;

        if (Timestamp.class.equals(object.getClass())) {
            return ((Timestamp) object).toLocalDateTime();
        } else if (byte[].class.equals(object.getClass())) {
            return new String((byte[]) object);
        }
        return object;
    }

    @Override
    public List<TDengineSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void addSplits(List<TDengineSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("no more split accepted!");
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing
    }
}
