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

package org.apache.seatunnel.connectors.bigquery.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.bigquery.client.BigQueryClientFactory;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySourceOptions;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class BigQuerySourceReader implements SourceReader<SeaTunnelRow, BigQuerySourceSplit> {

    private final SourceReader.Context context;
    private final ReadonlyConfig config;

    private final Deque<BigQuerySourceSplit> splits = new ConcurrentLinkedDeque<>();
    private volatile boolean noMoreSplit;

    private BigQuery bigQuery;
    private Iterator<FieldValueList> currentIterator;
    private FieldList currentFields;
    private BigQuerySourceSplit currentSplit;

    public BigQuerySourceReader(SourceReader.Context context, ReadonlyConfig config) {
        this.context = context;
        this.config = config;
    }

    public BigQuerySourceReader(
            SourceReader.Context context, ReadonlyConfig config, BigQuery bigQuery) {
        this.context = context;
        this.config = config;
        this.bigQuery = bigQuery;
    }

    @Override
    public void open() throws Exception {
        if (bigQuery == null) {
            bigQuery = BigQueryClientFactory.getBigQuery(config);
        }
    }

    @Override
    public void close() throws IOException {
        // The BigQuery client lifecycle is managed externally;
        // no resource to close here.
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // no-op
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            if (currentIterator == null) {
                BigQuerySourceSplit split = splits.poll();
                if (split == null) {
                    if (noMoreSplit) {
                        log.info("Closed the bounded BigQuery source");
                        context.signalNoMoreElement();
                    } else {
                        Thread.sleep(1000L);
                    }
                    return;
                }
                if (config.get(BigQuerySourceOptions.DRY_RUN)) {
                    executeDryRun(split.getQuery());
                    log.info("Dry run completed, no rows emitted");
                    context.signalNoMoreElement();
                    return;
                }
                currentSplit = split;
                TableResult tableResult = executeQuery(split.getQuery());
                Schema schema = tableResult.getSchema();
                currentFields = schema != null ? schema.getFields() : FieldList.of();
                currentIterator = tableResult.iterateAll().iterator();
            }

            int fetchSize = config.get(BigQuerySourceOptions.FETCH_SIZE);
            int emitted = 0;
            while (currentIterator.hasNext() && (fetchSize <= 0 || emitted < fetchSize)) {
                FieldValueList row = currentIterator.next();
                output.collect(convertRow(row, currentFields));
                emitted++;
            }
            if (!currentIterator.hasNext()) {
                currentIterator = null;
                currentFields = null;
                currentSplit = null;
            }
        }
    }

    @Override
    public List<BigQuerySourceSplit> snapshotState(long checkpointId) {
        if (currentSplit != null) {
            List<BigQuerySourceSplit> result = new ArrayList<>(splits);
            result.add(currentSplit);
            return result;
        }
        return List.copyOf(splits);
    }

    @Override
    public void addSplits(List<BigQuerySourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        this.noMoreSplit = true;
    }

    private TableResult executeQuery(String query) throws Exception {
        QueryJobConfiguration.Builder configBuilder =
                QueryJobConfiguration.newBuilder(query)
                        .setMaxResults(config.get(BigQuerySourceOptions.MAX_RESULTS))
                        .setUseQueryCache(config.get(BigQuerySourceOptions.USE_QUERY_CACHE));

        if (config.get(BigQuerySourceOptions.MAXIMUM_BYTES_BILLED) != null) {
            configBuilder.setMaximumBytesBilled(
                    config.get(BigQuerySourceOptions.MAXIMUM_BYTES_BILLED));
        }
        if (config.get(BigQuerySourceOptions.DATASET_ID) != null) {
            configBuilder.setDefaultDataset(
                    DatasetId.of(
                            config.get(BigQuerySourceOptions.PROJECT_ID),
                            config.get(BigQuerySourceOptions.DATASET_ID)));
        }

        QueryJobConfiguration queryConfig = configBuilder.build();

        try {
            if (config.get(BigQuerySourceOptions.LOCATION) != null) {
                JobId jobId =
                        JobId.newBuilder()
                                .setLocation(config.get(BigQuerySourceOptions.LOCATION))
                                .build();
                return bigQuery.query(queryConfig, jobId);
            }
            return bigQuery.query(queryConfig);
        } catch (BigQueryException e) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.READ_FAILED,
                    "Failed to execute BigQuery query: " + query,
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.READ_FAILED,
                    "Failed to execute BigQuery query: " + query,
                    e);
        }
    }

    private void executeDryRun(String query) {
        QueryJobConfiguration.Builder configBuilder =
                QueryJobConfiguration.newBuilder(query)
                        .setDryRun(true)
                        .setUseQueryCache(config.get(BigQuerySourceOptions.USE_QUERY_CACHE));

        if (config.get(BigQuerySourceOptions.MAXIMUM_BYTES_BILLED) != null) {
            configBuilder.setMaximumBytesBilled(
                    config.get(BigQuerySourceOptions.MAXIMUM_BYTES_BILLED));
        }
        if (config.get(BigQuerySourceOptions.DATASET_ID) != null) {
            configBuilder.setDefaultDataset(
                    DatasetId.of(
                            config.get(BigQuerySourceOptions.PROJECT_ID),
                            config.get(BigQuerySourceOptions.DATASET_ID)));
        }

        QueryJobConfiguration queryConfig = configBuilder.build();

        try {
            JobInfo jobInfo;
            if (config.get(BigQuerySourceOptions.LOCATION) != null) {
                JobId jobId =
                        JobId.newBuilder()
                                .setLocation(config.get(BigQuerySourceOptions.LOCATION))
                                .build();
                jobInfo = JobInfo.of(jobId, queryConfig);
            } else {
                jobInfo = JobInfo.of(queryConfig);
            }
            Job job = bigQuery.create(jobInfo);
            JobStatistics.QueryStatistics stats = job.getStatistics();
            if (stats != null) {
                log.info(
                        "Dry run for query: {}. Estimated bytes processed: {}",
                        query,
                        stats.getEstimatedBytesProcessed());
                Long cap = config.get(BigQuerySourceOptions.MAXIMUM_BYTES_BILLED);
                if (cap != null && stats.getEstimatedBytesProcessed() != null) {
                    if (stats.getEstimatedBytesProcessed() > cap) {
                        log.warn(
                                "Estimated bytes {} exceeds maximum_bytes_billed {}",
                                stats.getEstimatedBytesProcessed(),
                                cap);
                    }
                }
            }
        } catch (BigQueryException e) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.READ_FAILED,
                    "Failed to execute dry run for query: " + query,
                    e);
        }
    }

    private SeaTunnelRow convertRow(FieldValueList row, FieldList fields) {
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            FieldValue fieldValue = row.get(i);
            values[i] = convertFieldValue(fieldValue, field);
        }
        return new SeaTunnelRow(values);
    }

    private Object convertFieldValue(FieldValue value, Field field) {
        if (value.isNull()) {
            return null;
        }
        boolean isRepeated = Field.Mode.REPEATED.equals(field.getMode());
        if (field.getType() == null) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.READ_FAILED,
                    "Cannot determine BigQuery field type for '"
                            + field.getName()
                            + "': field type is null.");
        }
        StandardSQLTypeName bqType = field.getType().getStandardType();

        if (isRepeated) {
            return convertRepeatedValue(value, field);
        }
        if (bqType == StandardSQLTypeName.STRUCT) {
            FieldValueList record = value.getRecordValue();
            return convertRow(record, field.getSubFields());
        }
        return convertScalarValue(value, bqType);
    }

    private Object convertScalarValue(FieldValue value, StandardSQLTypeName bqType) {
        switch (bqType) {
            case BOOL:
                return value.getBooleanValue();
            case INT64:
                return value.getLongValue();
            case FLOAT64:
                return value.getDoubleValue();
            case NUMERIC:
            case BIGNUMERIC:
                return value.getNumericValue();
            case STRING:
                return value.getStringValue();
            case BYTES:
                return Base64.getDecoder().decode(value.getStringValue());
            case DATE:
                return LocalDate.parse(value.getStringValue());
            case TIME:
                return LocalTime.parse(value.getStringValue());
            case TIMESTAMP:
                long micros = value.getTimestampValue();
                return LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(
                                micros / 1_000_000, (int) ((micros % 1_000_000) * 1000)),
                        ZoneOffset.UTC);
            case DATETIME:
                return LocalDateTime.parse(value.getStringValue());
            default:
                return value.getStringValue();
        }
    }

    private Object convertRepeatedValue(FieldValue value, Field field) {
        Object rawValue = value.getValue();
        if (!(rawValue instanceof List)) {
            log.warn(
                    "Expected a List for REPEATED field '{}' but got {}. Returning null.",
                    field.getName(),
                    rawValue == null ? "null" : rawValue.getClass().getName());
            return null;
        }
        List<?> rawList = (List<?>) rawValue;
        Object[] array = new Object[rawList.size()];
        StandardSQLTypeName elementType = field.getType().getStandardType();
        FieldList subFields =
                elementType == StandardSQLTypeName.STRUCT ? field.getSubFields() : null;
        for (int i = 0; i < rawList.size(); i++) {
            Object elem = rawList.get(i);
            if (elem instanceof FieldValue) {
                FieldValue fv = (FieldValue) elem;
                if (elementType == StandardSQLTypeName.STRUCT) {
                    array[i] = convertRow(fv.getRecordValue(), subFields);
                } else {
                    array[i] = convertScalarValue(fv, elementType);
                }
            } else {
                array[i] = convertRawScalar(elem, elementType);
            }
        }
        return array;
    }

    private Object convertRawScalar(Object raw, StandardSQLTypeName bqType) {
        if (raw == null) {
            return null;
        }
        switch (bqType) {
            case BOOL:
                return Boolean.parseBoolean(raw.toString());
            case INT64:
                return Long.parseLong(raw.toString());
            case FLOAT64:
                return Double.parseDouble(raw.toString());
            case NUMERIC:
            case BIGNUMERIC:
                return new BigDecimal(raw.toString());
            case STRING:
                return raw.toString();
            case BYTES:
                return Base64.getDecoder().decode(raw.toString());
            case DATE:
                return LocalDate.parse(raw.toString());
            case TIME:
                return LocalTime.parse(raw.toString());
            case TIMESTAMP:
                if (raw instanceof Number) {
                    long micros = ((Number) raw).longValue();
                    return LocalDateTime.ofInstant(
                            Instant.ofEpochSecond(
                                    micros / 1_000_000, (int) ((micros % 1_000_000) * 1000)),
                            ZoneOffset.UTC);
                }
                return LocalDateTime.parse(raw.toString());
            case DATETIME:
                return LocalDateTime.parse(raw.toString());
            default:
                return raw.toString();
        }
    }
}
