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

package org.apache.seatunnel.flink.jdbc.input;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;

public class JdbcInputFormat extends org.apache.flink.connector.jdbc.JdbcInputFormat {

    @Override
    public Row nextRecord(Row reuse) throws IOException {
        try {
            if (!hasNext) {
                return null;
            }
            for (int pos = 0; pos < reuse.getArity(); pos++) {

                Object v = resultSet.getObject(pos + 1);
                if (Objects.isNull(v)) {
                    reuse.setField(pos, null);
                    continue;
                }
                TypeInformation<?> type = this.rowTypeInfo.getTypeAt(pos);
                Class<?> clazz = type.getTypeClass();
                if (String.class == clazz) {
                    reuse.setField(pos, String.valueOf(v));
                } else if (Boolean.class == clazz) {
                    reuse.setField(pos, resultSet.getBoolean(pos + 1));
                } else if (Byte.class == clazz) {
                    reuse.setField(pos, resultSet.getByte(pos + 1));
                } else if (Short.class == clazz) {
                    reuse.setField(pos, resultSet.getShort(pos + 1));
                } else if (Integer.class == clazz) {
                    reuse.setField(pos, resultSet.getInt(pos + 1));
                } else if (Long.class == clazz) {
                    reuse.setField(pos, resultSet.getLong(pos + 1));
                } else if (Float.class == clazz) {
                    reuse.setField(pos, resultSet.getFloat(pos + 1));
                } else if (Double.class == clazz) {
                    reuse.setField(pos, resultSet.getDouble(pos + 1));
                } else if (Character.class == clazz) {
                    reuse.setField(pos, resultSet.getString(pos + 1));
                } else if (Date.class == clazz) {
                    reuse.setField(pos, resultSet.getDate(pos + 1));
                } else if (BigInteger.class == clazz) {
                    reuse.setField(pos, resultSet.getString(pos + 1));
                } else if (Instant.class == clazz) {
                    reuse.setField(pos, resultSet.getString(pos + 1));
                } else if (java.sql.Date.class == clazz) {
                    reuse.setField(pos, resultSet.getDate(pos + 1));
                } else if (Time.class == clazz) {
                    reuse.setField(pos, resultSet.getTime(pos + 1));
                } else if (BigDecimal.class == clazz) {
                    reuse.setField(pos, resultSet.getBigDecimal(pos + 1));
                } else if (Timestamp.class == clazz) {
                    reuse.setField(pos, resultSet.getTimestamp(pos + 1));
                } else if (byte[].class == clazz) {
                    reuse.setField(pos, resultSet.getBytes(pos + 1));
                } else {
                    reuse.setField(pos, resultSet.getObject(pos + 1));
                }

            }
            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return reuse;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (NullPointerException npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }

    /**
     * override org.apache.flink.connector.jdbc.JdbcInputFormat.buildJdbcInputFormat()
     */
    public static JdbcInputFormatBuilder buildFlinkJdbcInputFormat() {
        return new JdbcInputFormatBuilder();
    }

    /**
     * Builder for {@link JdbcInputFormat}.
     */
    public static class JdbcInputFormatBuilder {
        private final JdbcConnectionOptions.JdbcConnectionOptionsBuilder connOptionsBuilder;
        private final JdbcInputFormat format;

        public JdbcInputFormatBuilder() {
            this.connOptionsBuilder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
            this.format = new JdbcInputFormat();
            // using TYPE_FORWARD_ONLY for high performance reads
            this.format.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
            this.format.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        }

        public JdbcInputFormatBuilder setUsername(String username) {
            connOptionsBuilder.withUsername(username);
            return this;
        }

        public JdbcInputFormatBuilder setPassword(String password) {
            connOptionsBuilder.withPassword(password);
            return this;
        }

        public JdbcInputFormatBuilder setDrivername(String drivername) {
            connOptionsBuilder.withDriverName(drivername);
            return this;
        }

        public JdbcInputFormatBuilder setDBUrl(String dbURL) {
            connOptionsBuilder.withUrl(dbURL);
            return this;
        }

        public JdbcInputFormatBuilder setQuery(String query) {
            format.queryTemplate = query;
            return this;
        }

        public JdbcInputFormatBuilder setResultSetType(int resultSetType) {
            format.resultSetType = resultSetType;
            return this;
        }

        public JdbcInputFormatBuilder setResultSetConcurrency(int resultSetConcurrency) {
            format.resultSetConcurrency = resultSetConcurrency;
            return this;
        }

        public JdbcInputFormatBuilder setParametersProvider(JdbcParameterValuesProvider parameterValuesProvider) {
            format.parameterValues = parameterValuesProvider.getParameterValues();
            return this;
        }

        public JdbcInputFormatBuilder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
            format.rowTypeInfo = rowTypeInfo;
            return this;
        }

        public JdbcInputFormatBuilder setFetchSize(int fetchSize) {
            Preconditions.checkArgument(fetchSize == Integer.MIN_VALUE || fetchSize > 0, "Illegal value %s for fetchSize, has to be positive or Integer.MIN_VALUE.", fetchSize);
            format.fetchSize = fetchSize;
            return this;
        }

        public JdbcInputFormatBuilder setAutoCommit(Boolean autoCommit) {
            format.autoCommit = autoCommit;
            return this;
        }

        public JdbcInputFormat finish() {
            format.connectionProvider = new SimpleJdbcConnectionProvider(connOptionsBuilder.build());
            if (format.queryTemplate == null) {
                throw new NullPointerException("No query supplied");
            }
            if (format.rowTypeInfo == null) {
                throw new NullPointerException("No " + RowTypeInfo.class.getSimpleName() + " supplied");
            }
            if (format.parameterValues == null) {
                LOG.debug("No input splitting configured (data will be read with parallelism 1).");
            }
            return format;
        }
    }
}
