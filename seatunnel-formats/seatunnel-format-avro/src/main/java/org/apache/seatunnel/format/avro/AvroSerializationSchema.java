/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.avro;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.avro.exception.AvroFormatErrorCode;
import org.apache.seatunnel.format.avro.exception.SeaTunnelAvroFormatException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroSerializationSchema implements SerializationSchema {

    private static final long serialVersionUID = 4438784443025715370L;

    private final ByteArrayOutputStream out;
    private final BinaryEncoder encoder;
    private final RowToAvroConverter converter;
    private final DatumWriter<GenericRecord> writer;

    public AvroSerializationSchema(SeaTunnelRowType rowType) {
        this.out = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(out, null);
        this.converter = new RowToAvroConverter(rowType);
        this.writer = this.converter.getWriter();
    }

    @Override
    public byte[] serialize(SeaTunnelRow element) {
        GenericRecord record = converter.convertRowToGenericRecord(element);
        try {
            writer.write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new SeaTunnelAvroFormatException(
                    AvroFormatErrorCode.SERIALIZATION_ERROR,
                    "Serialization error on record : " + element);
        } finally {
            out.reset();
        }
    }
}
