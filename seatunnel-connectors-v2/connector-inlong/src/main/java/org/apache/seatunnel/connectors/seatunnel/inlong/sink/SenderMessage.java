/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.inlong.sink;

import org.apache.commons.collections.CollectionUtils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/** A batch of seatunnel row used for batch sending, produced by InlongSinkWriter */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SenderMessage {

    private String groupId;
    private String streamId;
    private List<byte[]> dataList;
    private long dataTime;
    private Map<String, String> extraMap;

    public long getTotalSize() {
        return CollectionUtils.isEmpty(dataList)
                ? 0
                : dataList.stream().mapToLong(body -> body.length).sum();
    }
}
