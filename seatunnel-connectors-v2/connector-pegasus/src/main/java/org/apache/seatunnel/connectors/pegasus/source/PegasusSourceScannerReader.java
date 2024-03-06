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

package org.apache.seatunnel.connectors.pegasus.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pegasus.client.FilterType;
import org.apache.pegasus.client.PException;
import org.apache.pegasus.client.PegasusScannerInterface;
import org.apache.pegasus.client.ScanOptions;

import java.util.Base64;
import java.util.List;

import static org.apache.seatunnel.connectors.pegasus.source.PegasusSourceConfig.SCAN_OPTION_BATCH_SIZE;
import static org.apache.seatunnel.connectors.pegasus.source.PegasusSourceConfig.SCAN_OPTION_HASH_KEY_FILTER_PATTERN;
import static org.apache.seatunnel.connectors.pegasus.source.PegasusSourceConfig.SCAN_OPTION_HASH_KEY_FILTER_TYPE;
import static org.apache.seatunnel.connectors.pegasus.source.PegasusSourceConfig.SCAN_OPTION_NO_VALUE;
import static org.apache.seatunnel.connectors.pegasus.source.PegasusSourceConfig.SCAN_OPTION_SORT_KEY_FILTER_PATTERN;
import static org.apache.seatunnel.connectors.pegasus.source.PegasusSourceConfig.SCAN_OPTION_SORT_KEY_FILTER_TYPE;
import static org.apache.seatunnel.connectors.pegasus.source.PegasusSourceConfig.SCAN_OPTION_START_INCLUDE;
import static org.apache.seatunnel.connectors.pegasus.source.PegasusSourceConfig.SCAN_OPTION_STOP_INCLUDE;
import static org.apache.seatunnel.connectors.pegasus.source.PegasusSourceConfig.SCAN_OPTION_TIMEOUT_MILLIS;

public class PegasusSourceScannerReader extends PegasusSourceBaseReader {

    ScanOptions scanOptions;

    List<PegasusScannerInterface> scanners;

    public PegasusSourceScannerReader(Config pluginConfig, SingleSplitReaderContext context)
            throws PException {
        super(pluginConfig, context);
        ScanOptions scanOptions = new ScanOptions();
        if (pluginConfig.hasPath(SCAN_OPTION_TIMEOUT_MILLIS.key())) {
            scanOptions.timeoutMillis = pluginConfig.getInt(SCAN_OPTION_TIMEOUT_MILLIS.key());
        }
        if (pluginConfig.hasPath(SCAN_OPTION_BATCH_SIZE.key())) {
            scanOptions.batchSize = pluginConfig.getInt(SCAN_OPTION_BATCH_SIZE.key());
        }
        if (pluginConfig.hasPath(SCAN_OPTION_START_INCLUDE.key())) {
            scanOptions.startInclusive = pluginConfig.getBoolean(SCAN_OPTION_START_INCLUDE.key());
        }
        if (pluginConfig.hasPath(SCAN_OPTION_STOP_INCLUDE.key())) {
            scanOptions.stopInclusive = pluginConfig.getBoolean(SCAN_OPTION_STOP_INCLUDE.key());
        }
        if (pluginConfig.hasPath(SCAN_OPTION_HASH_KEY_FILTER_TYPE.key())) {
            scanOptions.hashKeyFilterType =
                    pluginConfig.getEnum(FilterType.class, SCAN_OPTION_HASH_KEY_FILTER_TYPE.key());
        }
        if (pluginConfig.hasPath(SCAN_OPTION_SORT_KEY_FILTER_TYPE.key())) {
            scanOptions.sortKeyFilterType =
                    pluginConfig.getEnum(FilterType.class, SCAN_OPTION_SORT_KEY_FILTER_TYPE.key());
        }
        if (pluginConfig.hasPath(SCAN_OPTION_HASH_KEY_FILTER_PATTERN.key())) {
            String hashKeyFilterPattern =
                    pluginConfig.getString(SCAN_OPTION_HASH_KEY_FILTER_PATTERN.key());
            scanOptions.hashKeyFilterPattern = Base64.getDecoder().decode(hashKeyFilterPattern);
        }
        if (pluginConfig.hasPath(SCAN_OPTION_SORT_KEY_FILTER_PATTERN.key())) {
            String sortKeyFilterPattern =
                    pluginConfig.getString(SCAN_OPTION_SORT_KEY_FILTER_PATTERN.key());
            scanOptions.sortKeyFilterPattern = Base64.getDecoder().decode(sortKeyFilterPattern);
        }
        if (pluginConfig.hasPath(PegasusSourceConfig.SCAN_OPTION_NO_VALUE.key())) {
            scanOptions.noValue = pluginConfig.getBoolean(SCAN_OPTION_NO_VALUE.key());
        }
        this.scanOptions = scanOptions;
    }

    @Override
    public void open() throws Exception {
        this.scanners = table.getUnorderedScanners(1, scanOptions);
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        for (PegasusScannerInterface scanner : scanners) {
            while (scanner.hasNext()) {
                Pair<Pair<byte[], byte[]>, byte[]> record = scanner.next();
                byte[] hashKey = record.getLeft().getLeft();
                byte[] sortKey = record.getLeft().getRight();
                byte[] value = record.getRight();
                output.collect(new SeaTunnelRow(new Object[] {hashKey, sortKey, value}));
            }
        }
        context.signalNoMoreElement();
    }
}
