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

package org.apache.seatunnel.connectors.seatunnel.console.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class ConsoleSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final SeaTunnelRowType seaTunnelRowType;
    public static final AtomicLong CNT = new AtomicLong(0);

    public ConsoleSinkWriter(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
        System.out.printf("files : %s%n", StringUtils.join(seaTunnelRowType.getFieldNames(), ", "));
        System.out.printf("types : %s%n", StringUtils.join(seaTunnelRowType.getFieldNames(), ", "));
    }

    @Override
    @SuppressWarnings("checkstyle:RegexpSingleline")
    public void write(SeaTunnelRow element) {
        String[] arr = new String[seaTunnelRowType.getTotalFields()];
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        Object[] fields = element.getFields();
        for (int i = 0; i < fieldTypes.length; i++) {
            if (i >= element.getArity()) {
                arr[i] = "null";
            } else {
                arr[i] = deepToString(fieldTypes[i], fields[i]);
            }
        }
        System.out.format("row=%s : %s%n", CNT.incrementAndGet(), StringUtils.join(arr, ", "));
    }

    @Override
    public void close() {
        // nothing
    }

    private String deepToString(SeaTunnelDataType<?> type, Object value) {
        switch (type.getSqlType()) {
            case ARRAY:
            case BYTES:
                return Arrays.toString((Object[]) value);
            case MAP:
                return JsonUtils.toJsonString(value);
            case ROW:
                return deepToString(type, value);
            default:
                return String.valueOf(value);
        }
    }
}
