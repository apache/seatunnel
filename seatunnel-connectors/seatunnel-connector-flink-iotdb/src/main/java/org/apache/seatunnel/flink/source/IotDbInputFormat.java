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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class IotDbInputFormat extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(IotDbInputFormat.class);

    private static final String IOTDB_DRIVER = "org.apache.iotdb.jdbc.IoTDBDriver";

    private String url;
    private String query;
    private List<String> fields;
    private RowTypeInfo rowTypeInfo;

    private transient Connection connection;
    private transient Statement statement;

    private long offset;
    private boolean hasNext;

    public IotDbInputFormat() {
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
            Class.forName(IOTDB_DRIVER);
            connection = DriverManager.getConnection(url, "root", "root");
            statement = connection.createStatement();
            offset = 0;
            hasNext = true;
        } catch (Exception e) {
            throw new IllegalArgumentException("openInputFormat() failed." + e.getMessage(), e);
        }
    }

    @Override
    public void closeInputFormat() {
        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (Exception e) {
            LOG.error("IotDbInputFormat couldn't be closed - " + e.getMessage());
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
            statement.execute(this.query + " LIMIT 1 OFFSET " + offset);
            final ResultSet resultSet = statement.getResultSet();
            if (resultSet == null) {
                hasNext = false;
                return null;
            }
            if (!resultSet.next()) {
                hasNext = false;
                return null;
            }
            for (int i = 0; i < fields.size(); i++) {
                row.setField(i, resultSet.getObject(i + 1));
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

    public static IotDbInputFormatBuilder buildIoTDbInputFormat() {
        return new IotDbInputFormatBuilder();
    }

    public static class IotDbInputFormatBuilder {
        private final IotDbInputFormat format;

        public IotDbInputFormatBuilder() {
            this.format = new IotDbInputFormat();
        }

        public IotDbInputFormatBuilder setURL(String url) {
            format.url = url;
            return this;
        }

        public IotDbInputFormatBuilder setQuery(String query) {
            format.query = query;
            return this;
        }

        public IotDbInputFormatBuilder setFields(List<String> fields) {
            format.fields = fields;
            return this;
        }

        public IotDbInputFormatBuilder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
            format.rowTypeInfo = rowTypeInfo;
            return this;
        }

        public IotDbInputFormat finish() {
            if (format.url == null) {
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
