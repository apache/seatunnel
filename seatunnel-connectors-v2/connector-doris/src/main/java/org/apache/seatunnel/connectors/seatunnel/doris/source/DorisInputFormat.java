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

package org.apache.seatunnel.connectors.seatunnel.doris.source;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.doris.util.JDBCUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * InputFormat to read data from a database and generate Rows. The InputFormat has to be configured
 * using the supplied InputFormatBuilder. A valid RowTypeInfo must be properly configured in the
 * builder
 */

public class DorisInputFormat implements Serializable {

    protected static final long serialVersionUID = 2L;
    protected static final Logger LOG = LoggerFactory.getLogger(DorisInputFormat.class);
    protected String database;
    protected String dorisbeaddress;
    protected String password;
    protected String username;
    protected String selectsql;
    protected SeaTunnelRowType rowTypeInfo;
    protected transient PreparedStatement statement;
    protected transient ResultSet resultSet;
    protected boolean hasNext;
    private Connection dbConn;

    public DorisInputFormat(String dorisbeaddress, String password, String username, String selectsql, String database, SeaTunnelRowType rowTypeInfo) {
        this.dorisbeaddress = dorisbeaddress;
        this.password = password;
        this.username = username;
        this.selectsql = selectsql;
        this.database = database;
        this.rowTypeInfo = rowTypeInfo;
    }

    public void openInputFormat() {
        // called once per inputFormat (on open)
        try {
            this.dbConn = JDBCUtils.getConnection(dorisbeaddress, username, password, database);
            statement = dbConn.prepareStatement(selectsql);

        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }
    }

    public void closeInputFormat() {
        // called once per inputFormat (on close)
        try {
            JDBCUtils.close(statement, dbConn);
        } catch (SQLException se) {
            LOG.info("Inputformat Statement couldn't be closed - " + se.getMessage());
        }
    }

    /**
     * Connects to the source database and executes the query
     *
     * @param sql SQL to query data
     * @throws IOException if there's an error during the execution of the query
     */
    public void open(String sql) throws IOException {
        try {

            resultSet = statement.executeQuery(sql);
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }
    }

    public void close() {
        if (resultSet == null) {
            return;
        }
        try {
            resultSet.close();
        } catch (SQLException se) {
            LOG.info("Inputformat ResultSet couldn't be closed - " + se.getMessage());
        }
    }

    /**
     * Checks whether all data has been read.
     *
     * @return boolean value indication whether all data has been read.
     */
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    /**
     * Convert a row of data to seatunnelRow
     */
    public SeaTunnelRow nextRecord() throws IOException {
        try {
            if (!hasNext) {
                return null;
            }
            SeaTunnelRow seaTunnelRow = toInternal(resultSet, rowTypeInfo);
            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return seaTunnelRow;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (NullPointerException npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }

    private SeaTunnelRow toInternal(ResultSet rs, SeaTunnelRowType typeInfo) throws SQLException {
        List<Object> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getFieldTypes();

        for (int i = 1; i <= seaTunnelDataTypes.length; i++) {
            Object seatunnelField;
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[i - 1];
            if (null == rs.getObject(i)) {
                seatunnelField = null;
            } else if (BasicType.BOOLEAN_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBoolean(i);
            } else if (BasicType.BYTE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getByte(i);
            } else if (BasicType.SHORT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getShort(i);
            } else if (BasicType.INT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getInt(i);
            } else if (BasicType.LONG_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getLong(i);
            } else if (seaTunnelDataType instanceof DecimalType) {
                Object value = rs.getObject(i);
                seatunnelField = value instanceof BigInteger ?
                    new BigDecimal((BigInteger) value, 0)
                    : value;
            } else if (BasicType.FLOAT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getFloat(i);
            } else if (BasicType.DOUBLE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getDouble(i);
            } else if (BasicType.STRING_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getString(i);
            } else if (LocalTimeType.LOCAL_TIME_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getTime(i).toLocalTime();
            } else if (LocalTimeType.LOCAL_DATE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getDate(i).toLocalDate();
            } else if (LocalTimeType.LOCAL_DATE_TIME_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getTimestamp(i).toLocalDateTime();
            } else if (PrimitiveByteArrayType.INSTANCE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBytes(i);
            } else {
                throw new IllegalStateException("Unexpected value: " + seaTunnelDataType);
            }

            fields.add(seatunnelField);
        }

        return new SeaTunnelRow(fields.toArray());
    }

}
