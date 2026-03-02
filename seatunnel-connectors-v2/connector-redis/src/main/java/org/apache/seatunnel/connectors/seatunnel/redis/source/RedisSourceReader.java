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

package org.apache.seatunnel.connectors.seatunnel.redis.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisClient;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisDataType;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;
import org.apache.seatunnel.connectors.seatunnel.redis.util.KeyValueMergerFactory;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class RedisSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final RedisParameters redisParameters;
    private final SingleSplitReaderContext context;
    private final Map<TablePath, RedisSourceTable> sourceTablesMap;
    private RedisClient redisClient;

    public RedisSourceReader(
            RedisParameters redisParameters,
            SingleSplitReaderContext context,
            Map<TablePath, RedisSourceTable> sourceTablesMap) {
        this.redisParameters = redisParameters;
        this.context = context;
        this.sourceTablesMap = sourceTablesMap;
    }

    @Override
    public void open() throws Exception {
        this.redisClient = redisParameters.buildRedisClient();
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(redisClient)) {
            redisClient.close();
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        // Process each redis table configuration
        for (Map.Entry<TablePath, RedisSourceTable> entry : sourceTablesMap.entrySet()) {
            TablePath tablePath = entry.getKey();
            RedisSourceTable sourceTable = entry.getValue();

            log.info(
                    "Processing redis table with TablePath: {}, key pattern: {}, data type: {}",
                    tablePath,
                    sourceTable.getKeyPattern(),
                    sourceTable.getDataType());
            processTable(sourceTable, output);
        }
        context.signalNoMoreElement();
    }

    /**
     * Process a single table by scanning all matching keys.
     *
     * @param sourceTable Source table configuration
     * @param output Collector for output rows
     * @throws Exception If error occurs during processing
     */
    private void processTable(RedisSourceTable sourceTable, Collector<SeaTunnelRow> output)
            throws Exception {
        String cursor = ScanParams.SCAN_POINTER_START;
        String keysPattern = sourceTable.getKeyPattern();
        int batchSize = sourceTable.getBatchSize();
        RedisDataType dataType = sourceTable.getDataType();
        RedisDataType scanType = resolveScanType(dataType);

        while (true) {
            // Scan keys matching the pattern
            ScanResult<String> scanResult =
                    redisClient.scanKeys(cursor, batchSize, keysPattern, scanType);
            cursor = scanResult.getCursor();
            List<String> keys = scanResult.getResult();

            // Process the batch of keys
            pollNext(sourceTable, keys, dataType, output);

            // Check if scan is complete (cursor returns "0")
            if (ScanParams.SCAN_POINTER_START.equals(cursor)) {
                break;
            }
        }
    }

    /**
     * Process a batch of keys and collect output rows.
     *
     * @param sourceTable Source table configuration
     * @param keys List of Redis keys to process
     * @param dataType Redis data type
     * @param output Collector for output rows
     * @throws IOException If error occurs during processing
     */
    private void pollNext(
            RedisSourceTable sourceTable,
            List<String> keys,
            RedisDataType dataType,
            Collector<SeaTunnelRow> output)
            throws IOException {
        if (CollectionUtils.isEmpty(keys)) {
            return;
        }

        // Create record reader for this table
        RedisRecordReader redisRecordReader = createRecordReader(sourceTable);

        // Process keys based on data type
        if (RedisDataType.HASH.equals(dataType)) {
            redisRecordReader.pollHashMapToNext(keys, output);
            return;
        }
        if (RedisDataType.STRING.equals(dataType) || RedisDataType.KEY.equals(dataType)) {
            redisRecordReader.pollStringToNext(keys, output);
            return;
        }
        if (RedisDataType.LIST.equals(dataType)) {
            redisRecordReader.pollListToNext(keys, output);
            return;
        }
        if (RedisDataType.SET.equals(dataType)) {
            redisRecordReader.pollSetToNext(keys, output);
            return;
        }
        if (RedisDataType.ZSET.equals(dataType)) {
            redisRecordReader.pollZsetToNext(keys, output);
            return;
        }
        throw new RedisConnectorException(
                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                "UnSupport redisDataType,only support string,list,hash,set,zset");
    }

    /**
     * Create a record reader for the given source table.
     *
     * @param sourceTable Source table configuration
     * @return RedisRecordReader
     */
    private RedisRecordReader createRecordReader(RedisSourceTable sourceTable) {
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                sourceTable.getDeserializationSchema();

        // Update this.redisParameters with table-specific KEY-related settings
        this.redisParameters.setFromSourceTable(sourceTable);

        if (Boolean.TRUE.equals(sourceTable.getReadKeyEnabled())) {
            return new KeyedRecordReader(
                    this.redisParameters,
                    deserializationSchema,
                    redisClient,
                    KeyValueMergerFactory.createMerger(
                            deserializationSchema, this.redisParameters));
        } else {
            return new UnKeyedRecordReader(
                    this.redisParameters, deserializationSchema, redisClient);
        }
    }

    /**
     * Resolve the scan type for Redis SCAN command. KEY type is mapped to STRING type for scanning.
     *
     * @param dataType Original data type
     * @return Scan type for Redis SCAN command
     */
    private RedisDataType resolveScanType(RedisDataType dataType) {
        if (RedisDataType.KEY.equals(dataType)) {
            return RedisDataType.STRING;
        }
        return dataType;
    }
}
