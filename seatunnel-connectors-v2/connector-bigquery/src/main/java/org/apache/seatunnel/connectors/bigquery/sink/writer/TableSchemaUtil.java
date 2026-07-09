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

package org.apache.seatunnel.connectors.bigquery.sink.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.bigquery.client.BigQueryClientFactory;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;

import org.threeten.bp.Duration;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;

import java.io.IOException;

import static org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions.IDENTIFIER;
import static org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryStreamWriter.CHANGE_TYPE;
import static org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryStreamWriter.SEQUENCE_NUM;

public class TableSchemaUtil {
    private TableSchemaUtil() {}

    static JsonStreamWriter createStreamWriter(
            String streamName, TableSchema tableSchema, BigQueryWriteClient client) {
        RetrySettings retrySettings =
                RetrySettings.newBuilder()
                        .setInitialRetryDelay(Duration.ofMillis(500))
                        .setRetryDelayMultiplier(1.1)
                        .setMaxAttempts(5)
                        .setMaxRetryDelay(Duration.ofMinutes(1))
                        .build();

        try {
            return JsonStreamWriter.newBuilder(streamName, tableSchema, client)
                    .setChannelProvider(
                            BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                                    .setKeepAliveTime(Duration.ofMinutes(1))
                                    .setKeepAliveTimeout(Duration.ofMinutes(1))
                                    .setKeepAliveWithoutCalls(true)
                                    .build())
                    .setFlowControlSettings(
                            FlowControlSettings.newBuilder()
                                    .setMaxOutstandingElementCount(100L)
                                    .build())
                    .setDefaultMissingValueInterpretation(
                            AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE)
                    .setRetrySettings(retrySettings)
                    .build();
        } catch (Descriptors.DescriptorValidationException | IOException e) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.WRITER_CREATE_FAILED, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.WRITER_CREATE_FAILED, e);
        }
    }

    static TableSchema getActualTableSchema(ReadonlyConfig config, boolean includeChangeTypeField) {
        String projectId = config.get(BigQuerySinkOptions.PROJECT_ID);
        String datasetId = config.get(BigQuerySinkOptions.DATASET_ID);
        String tableId = config.get(BigQuerySinkOptions.TABLE_ID);
        BigQuery bigquery = BigQueryClientFactory.getBigQuery(config);
        Table table = bigquery.getTable(TableId.of(projectId, datasetId, tableId));
        if (table == null) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.TABLE_NOT_FOUND,
                    String.format(
                            "BigQuery target table does not exist: %s.%s.%s. "
                                    + "Please create the target table before starting the sink.",
                            projectId, datasetId, tableId));
        }
        Schema bqSchema = table.getDefinition().getSchema();
        TableSchema.Builder builder = TableSchema.newBuilder();

        if (bqSchema == null || bqSchema.getFields() == null) {
            throw CommonError.illegalArgument(
                    String.format(
                            "Table %s.%s.%s does not exist or has no schema.",
                            projectId, datasetId, tableId),
                    IDENTIFIER);
        }

        for (Field field : bqSchema.getFields()) {
            if (field == null) {
                continue;
            }
            builder.addFields(convertField(field));
        }

        if (includeChangeTypeField) {
            boolean hasChangeType =
                    bqSchema.getFields().stream().anyMatch(f -> CHANGE_TYPE.equals(f.getName()));
            if (!hasChangeType) {
                builder.addFields(
                        TableFieldSchema.newBuilder()
                                .setName(CHANGE_TYPE)
                                .setType(TableFieldSchema.Type.STRING)
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .build());
            }

            boolean hasSequenceNum =
                    bqSchema.getFields().stream().anyMatch(f -> SEQUENCE_NUM.equals(f.getName()));
            if (!hasSequenceNum) {
                builder.addFields(
                        TableFieldSchema.newBuilder()
                                .setName(SEQUENCE_NUM)
                                .setType(TableFieldSchema.Type.INT64)
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .build());
            }
        }

        return builder.build();
    }

    static TableFieldSchema convertField(Field field) {
        TableFieldSchema.Builder fieldBuilder =
                TableFieldSchema.newBuilder()
                        .setName(field.getName())
                        .setMode(
                                TableFieldSchema.Mode.valueOf(
                                        field.getMode() != null
                                                ? field.getMode().name()
                                                : "NULLABLE"))
                        .setType(mapToStorageType(field.getType().getStandardType()));

        if (field.getType().getStandardType() == StandardSQLTypeName.STRUCT) {
            FieldList subFields = field.getSubFields();
            if (subFields != null) {
                for (Field subField : subFields) {
                    fieldBuilder.addFields(convertField(subField));
                }
            }
        }

        return fieldBuilder.build();
    }

    static TableFieldSchema.Type mapToStorageType(StandardSQLTypeName bqType) {
        switch (bqType) {
            case BOOL:
                return TableFieldSchema.Type.BOOL;
            case INT64:
                return TableFieldSchema.Type.INT64;
            case FLOAT64:
                return TableFieldSchema.Type.DOUBLE;
            case STRING:
                return TableFieldSchema.Type.STRING;
            case BYTES:
                return TableFieldSchema.Type.BYTES;
            case DATE:
                return TableFieldSchema.Type.DATE;
            case DATETIME:
                return TableFieldSchema.Type.DATETIME;
            case TIMESTAMP:
                return TableFieldSchema.Type.TIMESTAMP;
            case TIME:
                return TableFieldSchema.Type.TIME;
            case NUMERIC:
                return TableFieldSchema.Type.NUMERIC;
            case BIGNUMERIC:
                return TableFieldSchema.Type.BIGNUMERIC;
            case GEOGRAPHY:
                return TableFieldSchema.Type.GEOGRAPHY;
            case JSON:
                return TableFieldSchema.Type.JSON;
            case STRUCT:
                return TableFieldSchema.Type.STRUCT;
            default:
                throw CommonError.unsupportedDataType(IDENTIFIER, bqType.name(), bqType.toString());
        }
    }
}
