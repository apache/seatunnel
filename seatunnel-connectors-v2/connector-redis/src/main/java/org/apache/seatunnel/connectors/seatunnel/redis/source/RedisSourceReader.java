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
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisTableConfig;
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
    private final Map<TablePath, RedisTableConfig> sourceTablesMap;
    private RedisClient redisClient;

    public RedisSourceReader(
            RedisParameters redisParameters,
            SingleSplitReaderContext context,
            Map<TablePath, RedisTableConfig> sourceTablesMap) {
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
        for (Map.Entry<TablePath, RedisTableConfig> entry : sourceTablesMap.entrySet()) {
            TablePath tablePath = entry.getKey();
            RedisTableConfig tableConfig = entry.getValue();

            log.info(
                    "Processing redis table with TablePath: {}, key pattern: {}, data type: {}",
                    tablePath,
                    tableConfig.getKeys(),
                    tableConfig.getDataType());
            processTable(tableConfig, output);
        }
        context.signalNoMoreElement();
    }

    /**
     * Process a single table by scanning all matching keys.
     *
     * @param tableConfig Table configuration
     * @param output Collector for output rows
     * @throws Exception If error occurs during processing
     */
    private void processTable(RedisTableConfig tableConfig, Collector<SeaTunnelRow> output)
            throws Exception {
        String cursor = ScanParams.SCAN_POINTER_START;
        String keysPattern = tableConfig.getKeys();
        int batchSize = tableConfig.getBatchSize();
        RedisDataType dataType = tableConfig.getDataType();
        RedisDataType scanType = resolveScanType(dataType);

        int totalKeysScanned = 0;
        int scanIterations = 0;
        long startTime = System.currentTimeMillis();

        log.info(
                "Starting to scan table with key pattern: {}, batch size: {}",
                keysPattern,
                batchSize);

        RedisRecordReader redisRecordReader = createRecordReader(tableConfig);

        while (true) {
            scanIterations++;
            // Scan keys matching the pattern
            ScanResult<String> scanResult =
                    redisClient.scanKeys(cursor, batchSize, keysPattern, scanType);
            cursor = scanResult.getCursor();
            List<String> keys = scanResult.getResult();

            totalKeysScanned += keys.size();

            // Log progress once for every 100 keys scanned or every 10 iterations
            if (totalKeysScanned % 100 == 0 || scanIterations % 10 == 0) {
                log.info(
                        "Scanning progress for table {}: {} keys scanned in {} iterations",
                        tableConfig.getTablePath(),
                        totalKeysScanned,
                        scanIterations);
            }

            // Process the batch of keys
            pollNext(redisRecordReader, keys, dataType, output);

            // Check if scan is complete (cursor returns "0")
            if (ScanParams.SCAN_POINTER_START.equals(cursor)) {
                break;
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        log.info(
                "Finished scanning table {}: {} keys scanned in {} iterations, took {} ms",
                tableConfig.getTablePath(),
                totalKeysScanned,
                scanIterations,
                duration);
    }

    private void pollNext(
            RedisRecordReader redisRecordReader,
            List<String> keys,
            RedisDataType dataType,
            Collector<SeaTunnelRow> output)
            throws IOException {
        if (CollectionUtils.isEmpty(keys)) {
            return;
        }

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
     * Create a record reader for the given table configuration.
     *
     * @param tableConfig Table configuration
     * @return RedisRecordReader
     */
    private RedisRecordReader createRecordReader(RedisTableConfig tableConfig) {
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                tableConfig.getDeserializationSchema();
        TablePath tablePath = tableConfig.getTablePath();

        if (Boolean.TRUE.equals(tableConfig.getReadKeyEnabled())) {
            return new KeyedRecordReader(
                    this.redisParameters,
                    tableConfig,
                    deserializationSchema,
                    redisClient,
                    KeyValueMergerFactory.createMerger(deserializationSchema, tableConfig),
                    tablePath);
        } else {
            return new UnKeyedRecordReader(
                    this.redisParameters,
                    tableConfig,
                    deserializationSchema,
                    redisClient,
                    tablePath);
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
