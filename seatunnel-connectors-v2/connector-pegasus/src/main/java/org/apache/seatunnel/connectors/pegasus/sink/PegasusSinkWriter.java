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

package org.apache.seatunnel.connectors.pegasus.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import org.apache.pegasus.client.ClientOptions;
import org.apache.pegasus.client.PException;
import org.apache.pegasus.client.PegasusClientFactory;
import org.apache.pegasus.client.PegasusClientInterface;
import org.apache.pegasus.client.PegasusTableInterface;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PegasusSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    Logger logger = LoggerFactory.getLogger(PegasusSinkWriter.class);
    PegasusClientInterface client;
    PegasusTableInterface table;
    int hashKeyIdx = -1;
    int sortKeyIdx = -1;
    int valueIdx = -1;

    public PegasusSinkWriter(SeaTunnelRowType seaTunnelRowType, Config pluginConfig)
            throws PException {
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = fieldNames[i];
            switch (fieldName) {
                case "hash_key":
                    hashKeyIdx = i;
                    break;
                case "sort_key":
                    sortKeyIdx = i;
                    break;
                case "value":
                    valueIdx = i;
                    break;
                default:
                    break;
            }
        }
        if (hashKeyIdx == -1) {
            throw new IllegalArgumentException("cannot found 'hash_key' column!");
        }
        if (sortKeyIdx == -1) {
            throw new IllegalArgumentException("cannot found 'sort_key' column!");
        }
        if (valueIdx == -1) {
            throw new IllegalArgumentException("cannot found 'value' column!");
        }
        ClientOptions clientOptions =
                ClientOptions.builder()
                        .metaServers(pluginConfig.getString(PegasusSinkConfig.META_SERVER.key()))
                        // TODO
                        // .operationTimeout(1000)
                        .build();
        client = PegasusClientFactory.getSingletonClient(clientOptions);
        // todo session timeout?
        table = client.openTable(pluginConfig.getString(PegasusSinkConfig.TABLE.key()));
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        byte[] hashKey = (byte[]) row.getField(hashKeyIdx);
        byte[] sortKey = (byte[]) row.getField(sortKeyIdx);
        byte[] value = (byte[]) row.getField(valueIdx);
        logger.info("hashkey instance:" + hashKey);
        try {
            table.set(hashKey, sortKey, value, 10);
        } catch (PException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
