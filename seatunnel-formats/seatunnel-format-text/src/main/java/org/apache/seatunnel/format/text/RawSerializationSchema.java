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

package org.apache.seatunnel.format.text;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

public class RawSerializationSchema implements SerializationSchema {

    private final SeaTunnelRowType seaTunnelRowType;

    public RawSerializationSchema(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public byte[] serialize(SeaTunnelRow element) {
        if (element.getFields().length != seaTunnelRowType.getTotalFields()) {
            throw new IndexOutOfBoundsException(
                    "The data does not match the configured schema information, please check");
        }
        Object object = element.getField(0);
        if (object == null) {
            return "".getBytes();
        }
        return object.toString().getBytes();
    }
}
