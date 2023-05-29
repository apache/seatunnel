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

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.Builder;
import lombok.NonNull;

import java.io.IOException;

@Builder
public class RawDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private static final long serialVersionUID = 1L;

    @NonNull private SeaTunnelRowType seaTunnelRowType;

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        String content = new String(message);
        Object[] objects = new Object[seaTunnelRowType.getTotalFields()];
        objects[0] = content;
        return new SeaTunnelRow(objects);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }
}
