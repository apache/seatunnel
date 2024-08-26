/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.sls.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.sls.config.StartMode;
import org.apache.seatunnel.connectors.seatunnel.sls.serialization.FastLogDeserialization;

import com.aliyun.openservices.log.common.Consts;
import lombok.Data;

import java.io.Serializable;

@Data
public class ConsumerMetaData implements Serializable {
    private String project;
    private String logstore;
    private String consumerGroup;
    private StartMode startMode;
    private Consts.CursorMode autoCursorReset;
    private int fetchSize;
    private FastLogDeserialization<SeaTunnelRow> deserializationSchema;
    private CatalogTable catalogTable;
}
