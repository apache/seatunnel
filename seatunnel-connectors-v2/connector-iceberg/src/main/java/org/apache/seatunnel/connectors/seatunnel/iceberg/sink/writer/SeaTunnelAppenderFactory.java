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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.GenericParquetWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.util.SeaTunnelSchemaUtil;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Map;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkState;

public class SeaTunnelAppenderFactory implements FileAppenderFactory<SeaTunnelRow>, Serializable {
    private final Schema schema;
    private final SeaTunnelRowType seaTunnelSchema;
    private final Map<String, String> props;
    private final PartitionSpec spec;
    private final int[] equalityFieldIds;
    private final Schema eqDeleteRowSchema;
    private final Schema posDeleteRowSchema;
    private final Table table;

    private SeaTunnelRowType eqDeleteSeaTunnelSchema = null;
    private SeaTunnelRowType posDeleteSeaTunnelSchema = null;

    public SeaTunnelAppenderFactory(
            Table table,
            Schema schema,
            SeaTunnelRowType seaTunnelSchema,
            Map<String, String> props,
            PartitionSpec spec,
            int[] equalityFieldIds,
            Schema eqDeleteRowSchema,
            Schema posDeleteRowSchema) {
        checkNotNull(table, "Table shouldn't be null");
        this.table = table;
        this.schema = schema;
        this.seaTunnelSchema = seaTunnelSchema;
        this.props = props;
        this.spec = spec;
        this.equalityFieldIds = equalityFieldIds;
        this.eqDeleteRowSchema = eqDeleteRowSchema;
        this.posDeleteRowSchema = posDeleteRowSchema;
    }

    private SeaTunnelRowType lazyEqDeleteSeaTunnelSchema() {
        if (eqDeleteSeaTunnelSchema == null) {
            checkNotNull(eqDeleteRowSchema, "Equality delete row schema shouldn't be null");
            this.eqDeleteSeaTunnelSchema = SeaTunnelSchemaUtil.convert(eqDeleteRowSchema);
        }
        return eqDeleteSeaTunnelSchema;
    }

    private SeaTunnelRowType lazyPosDeleteFlinkSchema() {
        if (posDeleteSeaTunnelSchema == null) {
            checkNotNull(posDeleteRowSchema, "Pos-delete row schema shouldn't be null");
            this.posDeleteSeaTunnelSchema = SeaTunnelSchemaUtil.convert(posDeleteRowSchema);
        }
        return this.posDeleteSeaTunnelSchema;
    }

    @Override
    public FileAppender<SeaTunnelRow> newAppender(OutputFile outputFile, FileFormat format) {
        MetricsConfig metricsConfig = MetricsConfig.forTable(table);
        try {
            switch (format) {
                case PARQUET:
                    return Parquet.write(outputFile)
                            .createWriterFunc(GenericParquetWriter::buildWriter)
                            .setAll(props)
                            .metricsConfig(metricsConfig)
                            .schema(schema)
                            .overwrite()
                            .build();

                default:
                    throw new UnsupportedOperationException(
                            "Cannot write unknown file format: " + format);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public DataWriter<SeaTunnelRow> newDataWriter(
            EncryptedOutputFile file, FileFormat format, StructLike partition) {
        return new DataWriter<>(
                newAppender(file.encryptingOutputFile(), format),
                format,
                file.encryptingOutputFile().location(),
                spec,
                partition,
                file.keyMetadata());
    }

    @Override
    public EqualityDeleteWriter<SeaTunnelRow> newEqDeleteWriter(
            EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
        checkState(
                equalityFieldIds != null && equalityFieldIds.length > 0,
                "Equality field ids shouldn't be null or empty when creating equality-delete writer");
        checkNotNull(
                eqDeleteRowSchema,
                "Equality delete row schema shouldn't be null when creating equality-delete writer");

        MetricsConfig metricsConfig = MetricsConfig.forTable(table);
        try {
            switch (format) {
                case PARQUET:
                    return Parquet.writeDeletes(outputFile.encryptingOutputFile())
                            .createWriterFunc(GenericParquetWriter::buildWriter)
                            .withPartition(partition)
                            .overwrite()
                            .setAll(props)
                            .metricsConfig(metricsConfig)
                            .rowSchema(eqDeleteRowSchema)
                            .withSpec(spec)
                            .withKeyMetadata(outputFile.keyMetadata())
                            .equalityFieldIds(equalityFieldIds)
                            .buildEqualityWriter();

                default:
                    throw new UnsupportedOperationException(
                            "Cannot write equality-deletes for unsupported file format: " + format);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public PositionDeleteWriter<SeaTunnelRow> newPosDeleteWriter(
            EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
        MetricsConfig metricsConfig = MetricsConfig.forPositionDelete(table);
        try {
            switch (format) {
                case PARQUET:
                    return Parquet.writeDeletes(outputFile.encryptingOutputFile())
                            .createWriterFunc(GenericParquetWriter::buildWriter)
                            .withPartition(partition)
                            .overwrite()
                            .setAll(props)
                            .metricsConfig(metricsConfig)
                            .rowSchema(posDeleteRowSchema)
                            .withSpec(spec)
                            .withKeyMetadata(outputFile.keyMetadata())
                            .buildPositionWriter();

                default:
                    throw new UnsupportedOperationException(
                            "Cannot write pos-deletes for unsupported file format: " + format);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
