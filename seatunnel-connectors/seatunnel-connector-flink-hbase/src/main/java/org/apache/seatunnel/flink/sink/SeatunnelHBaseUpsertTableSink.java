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

package org.apache.seatunnel.flink.sink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.addons.hbase.HBaseTableSchema;
import org.apache.flink.addons.hbase.HBaseUpsertSinkFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.reflect.Invokable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.seatunnel.flink.source.HBaseSourceStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

public class SeatunnelHBaseUpsertTableSink implements UpsertStreamTableSink<Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSourceStream.class);

    private final HBaseTableSchema hbaseTableSchema;
    private final TableSchema tableSchema;
    private final Configuration hbaseConfiguration;
    private final WriteOptions writeOptions;
    private final String tableName;

    public SeatunnelHBaseUpsertTableSink(
            String tableName,
            HBaseTableSchema hbaseTableSchema,
            Configuration hbaseConiguration,
            WriteOptions writeOptions) {
        checkArgument(((Optional<String>) reflectHBaseTableSchemaInvokeMethod(hbaseTableSchema, "getRowKeyName"))
                .isPresent(), "HBaseUpsertTableSink requires rowkey is set.");
        checkArgument(StringUtils.isNotBlank(tableName), "HBaseUpsertTableSink requires tableName is not empty.");
        this.tableName = tableName;
        this.hbaseTableSchema = hbaseTableSchema;
        this.tableSchema = (TableSchema) reflectHBaseTableSchemaInvokeMethod(hbaseTableSchema, "convertsToTableSchema");
        this.hbaseConfiguration = hbaseConiguration;
        this.writeOptions = writeOptions;
    }

    private Object reflectHBaseTableSchemaInvokeMethod(HBaseTableSchema obj, String methodName) {
        try {
            Method method = HBaseTableSchema.class.getDeclaredMethod(methodName);
            method.setAccessible(true);
            Invokable<HBaseTableSchema, Object> methodInvoke = (Invokable<HBaseTableSchema, Object>) Invokable.from(method);
            return methodInvoke.invoke(obj);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException("execute HBaseTableSchema method: " + methodName + " failed");
        }
    }

    @Override
    public void setKeyFields(String[] strings) {
        // hbase always upsert on rowkey, ignore query keys.
        // Actually, we should verify the query key is the same with rowkey.
        // However, the query key extraction doesn't work well in some scenarios
        // (e.g. concat key fields will lose key information). So we skip key validation currently.
    }

    @Override
    public void setIsAppendOnly(Boolean aBoolean) {
        // hbase always upsert on rowkey, even works in append only mode.
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return tableSchema.toRowType();
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
            throw new ValidationException("Reconfiguration with different fields is not allowed. " +
                    "Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
                    "But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
        }

        return new SeatunnelHBaseUpsertTableSink(tableName, hbaseTableSchema, hbaseConfiguration, writeOptions);
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        HBaseUpsertSinkFunction sinkFunction = new HBaseUpsertSinkFunction(
                tableName,
                hbaseTableSchema,
                hbaseConfiguration,
                writeOptions.getBufferFlushMaxSizeInBytes(),
                writeOptions.getBufferFlushMaxRows(),
                writeOptions.getBufferFlushIntervalMillis());
        return dataStream
                .addSink(sinkFunction)
                .setParallelism(dataStream.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), tableSchema.getFieldNames()));
    }
}
