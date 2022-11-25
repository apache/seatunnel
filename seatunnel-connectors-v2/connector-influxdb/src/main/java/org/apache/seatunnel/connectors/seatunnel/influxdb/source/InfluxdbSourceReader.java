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

package org.apache.seatunnel.connectors.seatunnel.influxdb.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.influxdb.client.InfluxDBClient;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.converter.InfluxDBRowConverter;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

@Slf4j
public class InfluxdbSourceReader implements SourceReader<SeaTunnelRow, InfluxDBSourceSplit> {
    private InfluxDB influxdb;
    InfluxDBConfig config;

    private final SourceReader.Context context;

    private final SeaTunnelRowType seaTunnelRowType;

    List<Integer> columnsIndexList;
    private final Queue<InfluxDBSourceSplit> pendingSplits;

    private volatile boolean noMoreSplitsAssignment;

    InfluxdbSourceReader(InfluxDBConfig config, Context readerContext, SeaTunnelRowType seaTunnelRowType, List<Integer> columnsIndexList) {
        this.config = config;
        this.pendingSplits = new LinkedList<>();
        this.context = readerContext;
        this.seaTunnelRowType = seaTunnelRowType;
        this.columnsIndexList = columnsIndexList;
    }

    public void connect() throws ConnectException {
        if (influxdb == null) {
            influxdb = InfluxDBClient.getInfluxDB(config);
            String version = influxdb.version();
            if (!influxdb.ping().isGood()) {
                throw new InfluxdbConnectorException(InfluxdbConnectorErrorCode.CONNECT_FAILED,
                    String.format(
                        "connect influxdb failed, due to influxdb version info is unknown, the url is: {%s}",
                        config.getUrl()
                    )
                );
            }
            log.info("connect influxdb successful. sever version :{}.", version);
        }
    }

    @Override
    public void open() throws Exception {
        connect();
    }

    @Override
    public void close() {
        if (influxdb != null) {
            influxdb.close();
            influxdb = null;
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        while (!pendingSplits.isEmpty()) {
            synchronized (output.getCheckpointLock()) {
                InfluxDBSourceSplit split = pendingSplits.poll();
                read(split, output);
            }
        }

        if (Boundedness.BOUNDED.equals(context.getBoundedness())
            && noMoreSplitsAssignment
            && pendingSplits.isEmpty()) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded influxDB source");
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<InfluxDBSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<InfluxDBSourceSplit> splits) {
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader received NoMoreSplits event.");
        noMoreSplitsAssignment = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {

    }

    private void read(InfluxDBSourceSplit split, Collector<SeaTunnelRow> output) {
        QueryResult queryResult = influxdb.query(new Query(split.getQuery(), config.getDatabase()));
        for (QueryResult.Result result : queryResult.getResults()) {
            List<QueryResult.Series> serieList = result.getSeries();
            if (CollectionUtils.isNotEmpty(serieList)) {
                for (QueryResult.Series series : serieList) {
                    for (List<Object> values : series.getValues()) {
                        SeaTunnelRow row = InfluxDBRowConverter.convert(values, seaTunnelRowType, columnsIndexList);
                        output.collect(row);
                    }
                }
            } else {
                log.debug(
                    "split[{}] reader influxDB series is empty.", split.splitId());
            }
        }
    }
}
