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

package org.apache.seatunnel.connectors.seatunnel.amazonkinesis;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.amazonkinesis.KinesisConnection.Settings.bounded;

/** Produces append-only records for an existing stream. */
public final class AmazonKinesisSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {
    private static final long serialVersionUID = 1L;
    private final KinesisConnection.Settings connection;
    private final SinkSettings settings;
    private boolean checkpointing;

    AmazonKinesisSink(ReadonlyConfig options, CatalogTable table) {
        connection = new KinesisConnection.Settings(options);
        settings = new SinkSettings(options, table.getSeaTunnelRowType());
    }

    @Override
    public String getPluginName() {
        return AmazonKinesisSource.IDENTIFIER;
    }

    @Override
    public void setJobContext(JobContext context) {
        checkpointing = context.isEnableCheckpoint();
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
        KinesisWriter writer = new KinesisWriter(connection, settings, checkpointing);
        context.registerFlushAction(writer::flush);
        return writer;
    }

    static final class SinkSettings implements Serializable {
        private static final long serialVersionUID = 1L;
        final SeaTunnelRowType type;
        final String format;
        final int dataIndex;
        final int keyIndex;
        final String fixedKey;
        final int batchSize;
        final int batchBytes;
        final int recordBytes;
        final int attempts;
        final boolean ordered;

        SinkSettings(ReadonlyConfig options, SeaTunnelRowType type) {
            this.type = type;
            format = options.get(AmazonKinesisSinkOptions.FORMAT);
            String column = options.get(AmazonKinesisSinkOptions.DATA_FIELD);
            if ("json".equals(format) && (column == null || column.isEmpty())) {
                dataIndex = -1;
            } else if (column != null && ("binary".equals(format) || "text".equals(format))) {
                dataIndex =
                        field(
                                type,
                                column,
                                "binary".equals(format) ? SqlType.BYTES : SqlType.STRING);
            } else {
                throw new IllegalArgumentException(
                        "Kinesis format must be json or binary/text with a data_field");
            }
            fixedKey = options.get(AmazonKinesisSinkOptions.PARTITION_KEY);
            String keyField = options.get(AmazonKinesisSinkOptions.PARTITION_KEY_FIELD);
            if (fixedKey != null && keyField != null) {
                throw new IllegalArgumentException(
                        "Select either partition_key or partition_key_field");
            }
            if (fixedKey != null) {
                KinesisWriter.verifyKey(fixedKey);
            }
            keyIndex = keyField == null ? -1 : field(type, keyField, SqlType.STRING);
            ordered = options.get(AmazonKinesisSinkOptions.PRESERVE_ORDER);
            if (ordered && keyIndex < 0 && fixedKey == null) {
                throw new IllegalArgumentException(
                        "preserve_order requires a fixed or field partition key");
            }
            batchSize = bounded(options, AmazonKinesisSinkOptions.BATCH_SIZE, 1, 500);
            batchBytes = bounded(options, AmazonKinesisSinkOptions.BATCH_BYTES, 1, 10485760);
            recordBytes = bounded(options, AmazonKinesisSinkOptions.RECORD_BYTES, 1, batchBytes);
            attempts = bounded(options, AmazonKinesisSinkOptions.PARTIAL_ATTEMPTS, 1, 8);
        }

        private static int field(SeaTunnelRowType type, String column, SqlType required) {
            int index = type.indexOf(column, false);
            if (index < 0 || type.getFieldType(index).getSqlType() != required) {
                throw new IllegalArgumentException(
                        "Kinesis column is absent or has an incompatible type");
            }
            return index;
        }
    }
}
