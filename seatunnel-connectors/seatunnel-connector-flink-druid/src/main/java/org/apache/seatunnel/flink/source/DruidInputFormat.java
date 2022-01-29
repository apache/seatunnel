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
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

public class DruidInputFormat extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(DruidInputFormat.class);
    private static final long serialVersionUID = 6404870251026854042L;

    private String jdbcURL;
    private String query;
    private RowTypeInfo rowTypeInfo;

    private transient Connection dbConn;
    private transient PreparedStatement statement;
    private transient ResultSet resultSet;

    private boolean hasNext;
    private Object[][] parameterValues;

    public DruidInputFormat() {
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
            dbConn = DriverManager.getConnection(jdbcURL);
            statement = dbConn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        } catch (SQLException se) {
            throw new IllegalArgumentException("openInputFormat() failed." + se.getMessage(), se);
        }
    }

    @Override
    public void closeInputFormat() {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException se) {
            LOG.error("DruidInputFormat Statement couldn't be closed - " + se.getMessage());
        } finally {
            statement = null;
            try {
                if (dbConn != null) {
                    dbConn.close();
                }
            } catch (SQLException se) {
                LOG.error("DruidInputFormat couldn't be closed - " + se.getMessage());
            } finally {
                dbConn = null;
                parameterValues = null;
            }
        }
    }

    @Override
    public void open(InputSplit inputSplit) {
        try {
            if (inputSplit != null && parameterValues != null) {
                for (int i = 0; i < parameterValues[inputSplit.getSplitNumber()].length; i++) {
                    Object param = parameterValues[inputSplit.getSplitNumber()][i];
                    if (param instanceof String) {
                        statement.setString(i + 1, (String) param);
                    } else if (param instanceof Long) {
                        statement.setLong(i + 1, (Long) param);
                    } else if (param instanceof Integer) {
                        statement.setInt(i + 1, (Integer) param);
                    } else if (param instanceof Double) {
                        statement.setDouble(i + 1, (Double) param);
                    } else if (param instanceof Boolean) {
                        statement.setBoolean(i + 1, (Boolean) param);
                    } else if (param instanceof Float) {
                        statement.setFloat(i + 1, (Float) param);
                    } else if (param instanceof BigDecimal) {
                        statement.setBigDecimal(i + 1, (BigDecimal) param);
                    } else if (param instanceof Byte) {
                        statement.setByte(i + 1, (Byte) param);
                    } else if (param instanceof Short) {
                        statement.setShort(i + 1, (Short) param);
                    } else if (param instanceof Date) {
                        statement.setDate(i + 1, (Date) param);
                    } else if (param instanceof Time) {
                        statement.setTime(i + 1, (Time) param);
                    } else if (param instanceof Timestamp) {
                        statement.setTimestamp(i + 1, (Timestamp) param);
                    } else if (param instanceof Array) {
                        statement.setArray(i + 1, (Array) param);
                    } else {
                        throw new IllegalArgumentException("open() failed. Parameter " + i + " of type " + param.getClass() + " is not handled (yet).");
                    }
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Executing '{}' with parameters {}", query, Arrays.deepToString(parameterValues[inputSplit.getSplitNumber()]));
                }
            }
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }
    }

    @Override
    public void close() {
        if (resultSet == null) {
            return;
        }
        try {
            resultSet.close();
        } catch (SQLException se) {
            LOG.error("DruidInputFormat ResultSet couldn't be closed - " + se.getMessage());
        }
    }

    @Override
    public boolean reachedEnd() {
        return !hasNext;
    }

    @Override
    public Row nextRecord(Row row) throws IOException {
        try {
            if (!hasNext) {
                return null;
            }
            for (int pos = 0; pos < row.getArity(); pos++) {
                row.setField(pos, resultSet.getObject(pos + 1));
            }
            hasNext = resultSet.next();
            return row;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (NullPointerException npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) {
        if (parameterValues == null) {
            return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
        }
        GenericInputSplit[] ret = new GenericInputSplit[parameterValues.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new GenericInputSplit(i, ret.length);
        }
        return ret;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    public static DruidInputFormatBuilder buildDruidInputFormat() {
        return new DruidInputFormatBuilder();
    }

    public static class DruidInputFormatBuilder {
        private final DruidInputFormat format;

        public DruidInputFormatBuilder() {
            this.format = new DruidInputFormat();
        }

        public DruidInputFormatBuilder setDBUrl(String dbURL) {
            format.jdbcURL = dbURL;
            return this;
        }

        public DruidInputFormatBuilder setQuery(String query) {
            format.query = query;
            return this;
        }

        public DruidInputFormatBuilder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
            format.rowTypeInfo = rowTypeInfo;
            return this;
        }

        public DruidInputFormat finish() {
            if (format.jdbcURL == null) {
                throw new IllegalArgumentException("No database URL supplied");
            }
            if (format.query == null) {
                throw new IllegalArgumentException("No query supplied");
            }
            if (format.rowTypeInfo == null) {
                throw new IllegalArgumentException("No " + RowTypeInfo.class.getSimpleName() + " supplied");
            }
            if (format.parameterValues == null) {
                LOG.debug("No input splitting configured (data will be read with parallelism 1).");
            }
            return format;
        }
    }
}
