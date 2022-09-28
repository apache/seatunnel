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

package org.apache.seatunnel.connectors.seatunnel.mongodb.data;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.NonNull;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;

public class DefaultDeserializer implements Deserializer {

    private final SeaTunnelRowType rowType;
    private final Encoder encoder;

    public DefaultDeserializer(@NonNull SeaTunnelRowType rowType) {
        DataTypeValidator.validateDataType(rowType);
        this.rowType = rowType;
        this.encoder = new DocumentCodec();
    }

    @Override
    public SeaTunnelRow deserialize(Document document) {
        return convert(document);
    }

    private SeaTunnelRow convert(Document document) {
        SeaTunnelRow row = new SeaTunnelRow(rowType.getTotalFields());

        BsonWriter writer = new SeaTunnelRowBsonWriter(rowType, row);
        encoder.encode(writer, document, EncoderContext.builder().build());
        writer.flush();

        return row;
    }
}
