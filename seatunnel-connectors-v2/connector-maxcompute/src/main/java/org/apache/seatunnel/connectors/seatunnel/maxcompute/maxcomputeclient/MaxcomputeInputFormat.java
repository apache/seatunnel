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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.maxcomputeclient;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.ExceptionUtils;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MaxcomputeInputFormat implements Serializable {

    public MaxcomputeInputFormat(String aliyunOdpsAccountId, String aliyunOdpsAccountKey, String aliyunOdpsEndpoint
            , String aliyunOdpsDefaultProject, String maxcomputeTablePartition, String maxcomputeTableName) {
        this.aliyunOdpsAccountId = aliyunOdpsAccountId;
        this.aliyunOdpsAccountKey = aliyunOdpsAccountKey;
        this.aliyunOdpsEndpoint = aliyunOdpsEndpoint;
        this.aliyunOdpsDefaultProject = aliyunOdpsDefaultProject;
        this.maxcomputeTablePartition = maxcomputeTablePartition;
        this.maxcomputeTableName = maxcomputeTableName;
    }

    /**
     * Declare the global variable KuduClient and use it to manipulate the Kudu table
     */
    public Odps odps;
    public TableTunnel tunnel;

    /**
     * Specify kuduMaster address
     */
    public String aliyunOdpsAccountId;
    public String aliyunOdpsAccountKey;
    public String aliyunOdpsEndpoint;
    public String aliyunOdpsDefaultProject;
    public String maxcomputeTablePartition;
    public String maxcomputeTableName;
    public String kuduMaster;
    public static final int TIMEOUTMS = 18000;

    public TableSchema schema;
    Table table;

    /**
     * Specifies the name of the table
     */
    public List<Column> getColumnsSchemas() {
        return schema.getColumns();
    }

    public static SeaTunnelRow getSeaTunnelRowData(Record rs, SeaTunnelRowType typeInfo) throws SQLException {

        List<Object> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getFieldTypes();
        for (int i = 0; i < seaTunnelDataTypes.length; i++) {
            Object seatunnelField;
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[i];
            if (null == rs.get(i)) {
                seatunnelField = null;
            } else if (BasicType.BOOLEAN_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBoolean(i);
            } else if (BasicType.BYTE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBytes(i);
            } else if (BasicType.SHORT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBigint(i);
            } else if (BasicType.INT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBigint(i);
            } else if (BasicType.LONG_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBigint(i);
            } else if (seaTunnelDataType instanceof DecimalType) {
                Object value = rs.getDecimal(i);
                seatunnelField = value instanceof BigInteger ?
                        new BigDecimal((BigInteger) value, 0)
                        : value;
            } else if (BasicType.FLOAT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getDouble(i);
            } else if (BasicType.DOUBLE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getDouble(i);
            } else if (BasicType.STRING_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getString(i);
            } else {
                throw new IllegalStateException("Unexpected value: " + seaTunnelDataType);
            }
            fields.add(seatunnelField);
        }

        return new SeaTunnelRow(fields.toArray());
    }

    public SeaTunnelRowType getSeaTunnelRowType() {

        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            for (int i = 0; i < table.getSchema().getColumns().size(); i++) {
                fieldNames.add(table.getSchema().getColumns().get(i).getName());
                seaTunnelDataTypes.add(MaxcomputeTypeMapper.mapping(table.getSchema().getColumns(), i));
            }
        } catch (Exception e) {
            log.warn("get row type info exception.", e);
            throw new PrepareFailException("kudu", PluginType.SOURCE, ExceptionUtils.getMessage(e));
        }
        return new SeaTunnelRowType(fieldNames.toArray(new String[fieldNames.size()]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[seaTunnelDataTypes.size()]));
    }

    public void openInputFormat() {
        Account account = new AliyunAccount(aliyunOdpsAccountId, aliyunOdpsAccountKey);
        odps = new Odps(account);
        odps.setEndpoint(aliyunOdpsEndpoint);
        odps.setDefaultProject(aliyunOdpsDefaultProject);
        tunnel = new TableTunnel(odps);
        table = odps.tables().get(maxcomputeTableName);
        schema = table.getSchema();
        log.info("The Maxcompute client and tunnel is successfully initialized", aliyunOdpsEndpoint, aliyunOdpsDefaultProject);
    }

    public void closeInputFormat() {
        if (odps != null) {
            odps = null;
        }
        if (tunnel != null) {
            tunnel = null;
        }

    }

    public RecordReader getTunnelReader() {
        try {
            TableTunnel.DownloadSession downloadSession = tunnel.createDownloadSession(
                    aliyunOdpsDefaultProject, maxcomputeTableName);
            long count = downloadSession.getRecordCount();
            RecordReader recordReader = downloadSession.openRecordReader(0, count);
            return recordReader;
        } catch (TunnelException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            return null;
        }
    }
}
