/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.flink.source;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class InfluxDbInputFormat extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDbInputFormat.class);

    private String serverURL;
    private String username;
    private String password;
    private String database;
    private String query;
    private List<String> fields;
    private RowTypeInfo rowTypeInfo;

    private transient InfluxDB conn;

    private long offset;
    private boolean hasNext;

    public InfluxDbInputFormat() {
    }

    @Override
    public RowTypeInfo getProducedType() {
        return rowTypeInfo;
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void openInputFormat() {
        try {
            if (username == null || password == null) {
                conn = InfluxDBFactory.connect(serverURL);
            } else {
                conn = InfluxDBFactory.connect(serverURL, username, password);
            }
            conn.setDatabase(database);
            offset = 0;
            hasNext = true;
        } catch (Exception e) {
            throw new IllegalArgumentException("openInputFormat() failed." + e.getMessage(), e);
        }
    }

    @Override
    public void closeInputFormat() {
        try {
            if (conn != null) {
                conn.close();
                conn = null;
            }
        } catch (Exception e) {
            LOG.error("InfluxDbInputFormat couldn't be closed - " + e.getMessage());
        }
    }

    @Override
    public void open(InputSplit inputSplit) {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean reachedEnd() {
        return !hasNext;
    }

    @Override
    public Row nextRecord(Row row) throws IOException {
        try {
            QueryResult result = conn.query(new Query(this.query + " LIMIT 1 OFFSET " + offset));
            if (result == null) {
                hasNext = false;
                return null;
            }
            List<QueryResult.Result> results = result.getResults();
            if (CollectionUtils.isEmpty(results)) {
                hasNext = false;
                return null;
            }
            List<QueryResult.Series> series = results.get(0).getSeries();
            if (CollectionUtils.isEmpty(series)) {
                hasNext = false;
                return null;
            }
            List<Object> values = series.get(0).getValues().get(0);
            for (int i = 0; i < fields.size(); i++) {
                row.setField(i, values.get(i));
            }
            offset++;
            return row;
        } catch (Exception e) {
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) {
        return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    public static InfluxDbInputFormatBuilder buildInfluxDbInputFormat() {
        return new InfluxDbInputFormatBuilder();
    }

    public static class InfluxDbInputFormatBuilder {
        private final InfluxDbInputFormat format;

        public InfluxDbInputFormatBuilder() {
            this.format = new InfluxDbInputFormat();
        }

        public InfluxDbInputFormatBuilder setServerURL(String serverURL) {
            format.serverURL = serverURL;
            return this;
        }

        public InfluxDbInputFormatBuilder setUsername(String username) {
            format.username = username;
            return this;
        }

        public InfluxDbInputFormatBuilder setPassword(String password) {
            format.password = password;
            return this;
        }

        public InfluxDbInputFormatBuilder setDatabase(String database) {
            format.database = database;
            return this;
        }

        public InfluxDbInputFormatBuilder setQuery(String query) {
            format.query = query;
            return this;
        }

        public InfluxDbInputFormatBuilder setFields(List<String> fields) {
            format.fields = fields;
            return this;
        }

        public InfluxDbInputFormatBuilder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
            format.rowTypeInfo = rowTypeInfo;
            return this;
        }

        public InfluxDbInputFormat finish() {
            if (format.serverURL == null) {
                throw new IllegalArgumentException("No database URL supplied");
            }
            if (format.query == null) {
                throw new IllegalArgumentException("No query supplied");
            }
            if (format.rowTypeInfo == null) {
                throw new IllegalArgumentException("No " + RowTypeInfo.class.getSimpleName() + " supplied");
            }
            return format;
        }
    }
}
