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

package org.apache.seatunnel.spark.utils;

import org.apache.seatunnel.common.config.ConfigRuntimeException;
import org.apache.seatunnel.common.utils.JsonUtils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

public final class SparkStructTypeUtil {

    private SparkStructTypeUtil() {
    }

    public static StructType getStructType(StructType schema, ObjectNode json) {
        StructType newSchema = schema.copy(schema.fields());
        Map<String, Object> jsonMap = JsonUtils.toMap(json);
        for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
            String field = entry.getKey();
            Object type = entry.getValue();
            if (type instanceof ObjectNode) {
                StructType st = getStructType(new StructType(), (ObjectNode) type);
                newSchema = newSchema.add(field, st);
            } else if (type instanceof List) {
                List list = (List) type;

                if (list.size() == 0) {
                    newSchema = newSchema.add(field, DataTypes.createArrayType(null, true));
                } else {
                    Object o = list.get(0);
                    if (o instanceof ObjectNode) {
                        StructType st = getStructType(new StructType(), (ObjectNode) o);
                        newSchema = newSchema.add(field, DataTypes.createArrayType(st, true));
                    } else {
                        DataType st = getType(o.toString());
                        newSchema = newSchema.add(field, DataTypes.createArrayType(st, true));
                    }
                }

            } else {
                newSchema = newSchema.add(field, getType(type.toString()));
            }
        }
        return newSchema;
    }

    private static DataType getType(String type) {
        DataType dataType;
        switch (type.toLowerCase()) {
            case "string":
                dataType = DataTypes.StringType;
                break;
            case "integer":
                dataType = DataTypes.IntegerType;
                break;
            case "long":
                dataType = DataTypes.LongType;
                break;
            case "double":
                dataType = DataTypes.DoubleType;
                break;
            case "float":
                dataType = DataTypes.FloatType;
                break;
            case "short":
                dataType = DataTypes.ShortType;
                break;
            case "date":
                dataType = DataTypes.DateType;
                break;
            case "timestamp":
                dataType = DataTypes.TimestampType;
                break;
            case "boolean":
                dataType = DataTypes.BooleanType;
                break;
            case "binary":
                dataType = DataTypes.BinaryType;
                break;
            case "byte":
                dataType = DataTypes.ByteType;
                break;
            default:
                throw new ConfigRuntimeException("Throw data type exception, unknown type: " + type);
        }
        return dataType;
    }
}
