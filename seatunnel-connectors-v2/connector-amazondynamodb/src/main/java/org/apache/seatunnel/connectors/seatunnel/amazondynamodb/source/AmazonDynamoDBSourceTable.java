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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.source;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

/** A DynamoDB table read by the source, with the read options and row type used for it. */
@Getter
@AllArgsConstructor
class AmazonDynamoDBSourceTable implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Identity carried by splits and rows; null for a single-table source. */
    private final String tableId;

    private final AmazonDynamoDBConfig config;

    private final SeaTunnelRowType rowType;
}
