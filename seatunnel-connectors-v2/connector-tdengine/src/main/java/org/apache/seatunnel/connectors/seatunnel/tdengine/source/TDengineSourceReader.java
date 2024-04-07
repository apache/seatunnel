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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.tdengine.exception.TDengineConnectorException;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Sets;
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
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.tdengine.utils.TDengineUtil.checkDriverExist;

@Slf4j
public class TDengineSourceReader implements SourceReader<SeaTunnelRow, TDengineSourceSplit> {

    private static final long THREAD_WAIT_TIME = 500L;

    private final TDengineSourceConfig config;

    private final Set<TDengineSourceSplit> sourceSplits;

    private final Context context;

    private Connection conn;

    public TDengineSourceReader(TDengineSourceConfig config, SourceReader.Context readerContext) {
        this.config = config;
        this.sourceSplits = Sets.newHashSet();
        this.context = readerContext;
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> collector) throws InterruptedException {
        if (sourceSplits.isEmpty()) {
            Thread.sleep(THREAD_WAIT_TIME);
            return;
        }
        synchronized (collector.getCheckpointLock()) {
            sourceSplits.forEach(
                    split -> {
                        try {
                            read(split, collector);
                        } catch (Exception e) {
                            throw new TDengineConnectorException(
                                    CommonErrorCodeDeprecated.READER_OPERATION_FAILED,
                                    "TDengine split read error",
                                    e);
                        }
                    });
        }

        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded TDengine source");
            context.signalNoMoreElement();
        }
    }

    @Override
    public void open() {
        String jdbcUrl =
                StringUtils.join(
                        config.getUrl(),
                        config.getDatabase(),
                        "?user=",
                        config.getUsername(),
                        "&password=",
                        config.getPassword());
        Properties connProps = new Properties();
        // todo: when TSDBDriver.PROPERTY_KEY_BATCH_LOAD set to "true",
        // there is a exception : Caused by: java.sql.SQLException: can't create connection with
        // server
        // under docker network env
        // @bobo (tdengine)
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "false");
        try {
            // check td driver whether exist and if not, try to register
            checkDriverExist(jdbcUrl);
            conn = DriverManager.getConnection(jdbcUrl, connProps);
        } catch (SQLException e) {
            throw new TDengineConnectorException(
                    CommonErrorCodeDeprecated.READER_OPERATION_FAILED,
                    "get TDengine connection failed:" + jdbcUrl);
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
        try (Statement statement = conn.createStatement()) {
            final ResultSet resultSet = statement.executeQuery(split.getQuery());
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
        // do nothing
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing
    }
}
