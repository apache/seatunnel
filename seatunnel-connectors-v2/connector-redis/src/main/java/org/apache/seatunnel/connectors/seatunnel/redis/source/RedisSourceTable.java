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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisDataType;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisSourceOptions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * Encapsulates metadata for a single Redis table configuration. Each table configuration represents
 * a logical table corresponding to a specific key pattern in Redis.
 */
@Data
@Builder
@AllArgsConstructor
public class RedisSourceTable implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Unique identifier for this table configuration */
    private TablePath tablePath;

    /** Redis key pattern to scan (e.g., "user:*") */
    private String keyPattern;

    /** Redis data type (STRING, HASH, LIST, SET, ZSET) */
    private RedisDataType dataType;

    /** Batch size for SCAN operations */
    private int batchSize;

    /** SeaTunnel catalog table containing schema information */
    private CatalogTable catalogTable;

    /** Deserialization schema for converting Redis values to SeaTunnelRow */
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;

    /** HASH key parse mode (ALL or KV) */
    private RedisSourceOptions.HashKeyParseMode hashKeyParseMode;

    /** Whether to include Redis key in output */
    private Boolean readKeyEnabled;

    /** Field name for Redis key in output */
    private String keyFieldName;

    /** Field name for single-value Redis data types */
    private String singleFieldName;
}
