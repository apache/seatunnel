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

package org.apache.seatunnel.connectors.seatunnel.hbase.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HbaseSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private static final String ALL_COLUMNS = "all_columns";

    private final Configuration hbaseConfiguration = HBaseConfiguration.create();

    private final Connection hbaseConnection;

    private final BufferedMutator hbaseMutator;

    private final SeaTunnelRowType seaTunnelRowType;

    private final HbaseParameters hbaseParameters;

    private final List<Integer> rowkeyColumnIndexes;

    private final int versionColumnIndex;

    private String defaultFamilyName = "value";

    public HbaseSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            HbaseParameters hbaseParameters,
            List<Integer> rowkeyColumnIndexes,
            int versionColumnIndex)
            throws IOException {
        this.seaTunnelRowType = seaTunnelRowType;
        this.hbaseParameters = hbaseParameters;
        this.rowkeyColumnIndexes = rowkeyColumnIndexes;
        this.versionColumnIndex = versionColumnIndex;

        if (hbaseParameters.getFamilyNames().size() == 1) {
            defaultFamilyName = hbaseParameters.getFamilyNames().getOrDefault(ALL_COLUMNS, "value");
        }

        // initialize hbase configuration
        hbaseConfiguration.set("hbase.zookeeper.quorum", hbaseParameters.getZookeeperQuorum());
        if (hbaseParameters.getHbaseExtraConfig() != null) {
            hbaseParameters.getHbaseExtraConfig().forEach(hbaseConfiguration::set);
        }
        // initialize hbase connection
        hbaseConnection = ConnectionFactory.createConnection(hbaseConfiguration);
        // initialize hbase mutator
        BufferedMutatorParams bufferedMutatorParams =
                new BufferedMutatorParams(TableName.valueOf(hbaseParameters.getTable()))
                        .pool(HTable.getDefaultExecutor(hbaseConfiguration))
                        .writeBufferSize(hbaseParameters.getWriteBufferSize());
        hbaseMutator = hbaseConnection.getBufferedMutator(bufferedMutatorParams);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        Put put = convertRowToPut(element);
        hbaseMutator.mutate(put);
    }

    @Override
    public void close() throws IOException {
        if (hbaseMutator != null) {
            hbaseMutator.close();
        }
        if (hbaseConnection != null) {
            hbaseConnection.close();
        }
    }

    private Put convertRowToPut(SeaTunnelRow row) {
        byte[] rowkey = getRowkeyFromRow(row);
        long timestamp = System.currentTimeMillis();
        if (versionColumnIndex != -1) {
            timestamp = (Long) row.getField(versionColumnIndex);
        }
        Put put = new Put(rowkey, timestamp);
        if (!hbaseParameters.isWalWrite()) {
            put.setDurability(Durability.SKIP_WAL);
        }
        List<Integer> writeColumnIndexes =
                IntStream.range(0, row.getArity())
                        .boxed()
                        .filter(index -> !rowkeyColumnIndexes.contains(index))
                        .filter(index -> index != versionColumnIndex)
                        .collect(Collectors.toList());
        for (Integer writeColumnIndex : writeColumnIndexes) {
            String fieldName = seaTunnelRowType.getFieldName(writeColumnIndex);
            String familyName =
                    hbaseParameters.getFamilyNames().getOrDefault(fieldName, defaultFamilyName);
            byte[] bytes = convertColumnToBytes(row, writeColumnIndex);
            if (bytes != null) {
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(fieldName), bytes);
            } else {
                switch (hbaseParameters.getNullMode()) {
                    case EMPTY:
                        put.addColumn(
                                Bytes.toBytes(familyName),
                                Bytes.toBytes(fieldName),
                                HConstants.EMPTY_BYTE_ARRAY);
                        break;
                    case SKIP:
                    default:
                        break;
                }
            }
        }
        return put;
    }

    private byte[] getRowkeyFromRow(SeaTunnelRow row) {
        String[] rowkeyValues = new String[rowkeyColumnIndexes.size()];
        for (int i = 0; i < rowkeyColumnIndexes.size(); i++) {
            rowkeyValues[i] = row.getField(rowkeyColumnIndexes.get(i)).toString();
        }
        return Bytes.toBytes(String.join(hbaseParameters.getRowkeyDelimiter(), rowkeyValues));
    }

    private byte[] convertColumnToBytes(SeaTunnelRow row, int index) {
        Object field = row.getField(index);
        if (field == null) {
            return null;
        }
        SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(index);
        switch (fieldType.getSqlType()) {
            case TINYINT:
                return Bytes.toBytes((Byte) field);
            case SMALLINT:
                return Bytes.toBytes((Short) field);
            case INT:
                return Bytes.toBytes((Integer) field);
            case BIGINT:
                return Bytes.toBytes((Long) field);
            case FLOAT:
                return Bytes.toBytes((Float) field);
            case DOUBLE:
                return Bytes.toBytes((Double) field);
            case BOOLEAN:
                return Bytes.toBytes((Boolean) field);
            case STRING:
                return field.toString()
                        .getBytes(Charset.forName(hbaseParameters.getEnCoding().toString()));
            default:
                String errorMsg =
                        String.format(
                                "Hbase connector does not support this column type [%s]",
                                fieldType.getSqlType());
                throw new HbaseConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }
}
