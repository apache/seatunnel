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

package org.apache.seatunnel.connectors.seatunnel.lance.sink.writers;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;

public abstract class BaseTypeWriter implements TypeWriter {
    protected byte[] getBytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    protected ArrowBuf createArrowBuf(byte[] bytes, BufferAllocator allocator) {
        ArrowBuf buffer = allocator.buffer(bytes.length);
        buffer.writeBytes(bytes);
        return buffer;
    }

    protected boolean toBoolean(Object value) {
        return value instanceof Boolean ? (Boolean) value : Boolean.parseBoolean(value.toString());
    }

    protected long convertToEpochMicro(Object value) {
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
                    * 1000;
        } else if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).getTime() * 1000;
        } else if (value instanceof java.util.Date) {
            return ((java.util.Date) value).getTime() * 1000;
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported timestamp value type: " + value.getClass());
        }
    }
}
