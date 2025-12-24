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

package org.apache.seatunnel.connectors.seatunnel.sls.serialization;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.LogItem;

import java.util.ArrayList;
import java.util.List;

public class SeatunnelRowSerialization {
    JsonSerializationSchema jsonSerializationSchema;

    public SeatunnelRowSerialization(SeaTunnelRowType rowType) {
        this.jsonSerializationSchema = new JsonSerializationSchema(rowType);
    }

    public List<LogItem> serializeRow(SeaTunnelRow row) {
        List<LogItem> logGroup = new ArrayList<LogItem>();
        LogItem logItem = new LogItem();
        String rowJson = new String(jsonSerializationSchema.serialize(row));
        LogContent content = new LogContent("content", rowJson);
        logItem.PushBack(content);
        logGroup.add(logItem);
        return logGroup;
    }
}
