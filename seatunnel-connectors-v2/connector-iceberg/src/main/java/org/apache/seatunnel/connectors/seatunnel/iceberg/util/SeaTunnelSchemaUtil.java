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

package org.apache.seatunnel.connectors.seatunnel.iceberg.util;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;

public class SeaTunnelSchemaUtil {

    private SeaTunnelSchemaUtil() {}

    /**
     * Convert a {@link Schema} to a {@link SeaTunnelRowType}.
     *
     * @param schema a Schema
     * @return the equivalent Flink type
     * @throws IllegalArgumentException if the type cannot be converted to Flink
     */
    public static SeaTunnelRowType convert(Schema schema) {
        return (SeaTunnelRowType) TypeUtil.visit(schema, new TypeToSeatunnelType());
    }

    /**
     * Convert a {@link Type} to a {@link SeaTunnelRowType}.
     *
     * @param type a Type
     * @return the equivalent Flink type
     * @throws IllegalArgumentException if the type cannot be converted to Flink
     */
    public static SeaTunnelDataType<?> convert(Type type) {
        return TypeUtil.visit(type, new TypeToSeatunnelType());
    }
}
