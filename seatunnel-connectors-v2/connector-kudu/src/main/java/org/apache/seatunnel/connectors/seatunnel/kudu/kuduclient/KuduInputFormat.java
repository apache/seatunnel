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

package org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;

import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class KuduInputFormat implements Serializable {

    public KuduInputFormat(String kuduMaster, String tableName, String columnsList) {
        this.kuduMaster = kuduMaster;
        this.columnsList = Arrays.asList(columnsList.split(","));
        this.tableName = tableName;

    }

    /**
     * Declare the global variable KuduClient and use it to manipulate the Kudu table
     */
    public KuduClient kuduClient;

    /**
     * Specify kuduMaster address
     */
    public String kuduMaster;
    public List<String> columnsList;
    public Schema schema;
    public String keyColumn;
    public static final int TIMEOUTMS = 18000;

    /**
     * Specifies the name of the table
     */
    public String tableName;

    public List<ColumnSchema> getColumnsSchemas() {
        List<ColumnSchema> columns = null;
        try {
            schema = kuduClient.openTable(tableName).getSchema();
            keyColumn = schema.getPrimaryKeyColumns().get(0).getName();
            columns = schema.getColumns();
        } catch (KuduException e) {
            throw new KuduConnectorException(CommonErrorCode.TABLE_SCHEMA_GET_FAILED, "get table Columns Schemas Failed");
        }
        return columns;
    }

    public static SeaTunnelRow getSeaTunnelRowData(RowResult rs, SeaTunnelRowType typeInfo) throws SQLException {

        List<Object> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getFieldTypes();
        for (int i = 0; i < seaTunnelDataTypes.length; i++) {
            Object seatunnelField;
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[i];
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
            } else {
                throw new KuduConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported data type: " + seaTunnelDataType);
            }
            fields.add(seatunnelField);
        }

        return new SeaTunnelRow(fields.toArray());
    }

    public SeaTunnelRowType getSeaTunnelRowType(List<ColumnSchema> columnSchemaList) {

        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {

            for (int i = 0; i < columnSchemaList.size(); i++) {
                fieldNames.add(columnSchemaList.get(i).getName());
                seaTunnelDataTypes.add(KuduTypeMapper.mapping(columnSchemaList, i));
            }
        } catch (Exception e) {
            throw new KuduConnectorException(CommonErrorCode.TABLE_SCHEMA_GET_FAILED, String.format("PluginName: %s, PluginType: %s, Message: %s",
                    "Kudu", PluginType.SOURCE, ExceptionUtils.getMessage(e)));
        }
        return new SeaTunnelRowType(fieldNames.toArray(new String[fieldNames.size()]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[seaTunnelDataTypes.size()]));
    }

    public void openInputFormat() {
        KuduClient.KuduClientBuilder kuduClientBuilder = new
                KuduClient.KuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultOperationTimeoutMs(TIMEOUTMS);

        kuduClient = kuduClientBuilder.build();

        log.info("The Kudu client is successfully initialized", kuduMaster, kuduClient);

    }

    /**
     * @param lowerBound The beginning of each slice
     * @param upperBound End of each slice
     * @return Get the kuduScanner object for each slice
     */

    public KuduScanner getKuduBuildSplit(int lowerBound, int upperBound) {
        KuduScanner kuduScanner = null;
        try {
            KuduScanner.KuduScannerBuilder kuduScannerBuilder =
                    kuduClient.newScannerBuilder(kuduClient.openTable(tableName));

            kuduScannerBuilder.setProjectedColumnNames(columnsList);

            KuduPredicate lowerPred = KuduPredicate.newComparisonPredicate(
                    schema.getColumn("" + keyColumn),
                    KuduPredicate.ComparisonOp.GREATER_EQUAL,
                    lowerBound);

            KuduPredicate upperPred = KuduPredicate.newComparisonPredicate(
                    schema.getColumn("" + keyColumn),
                    KuduPredicate.ComparisonOp.LESS,
                    upperBound);

            kuduScanner = kuduScannerBuilder.addPredicate(lowerPred)
                    .addPredicate(upperPred).build();
        } catch (KuduException e) {
            throw new KuduConnectorException(KuduConnectorErrorCode.GET_KUDUSCAN_OBJECT_FAILED, e);
        }
        return kuduScanner;
    }

    public void closeInputFormat() {
        if (kuduClient != null) {
            try {
                kuduClient.close();
            } catch (KuduException e) {
                throw new KuduConnectorException(KuduConnectorErrorCode.CLOSE_KUDU_CLIENT_FAILED, e);
            } finally {
                kuduClient = null;
            }
        }

    }
}
