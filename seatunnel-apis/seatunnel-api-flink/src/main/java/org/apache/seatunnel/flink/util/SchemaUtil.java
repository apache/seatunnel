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

package org.apache.seatunnel.flink.util;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.utils.TypeStringUtils;
import org.apache.flink.types.Row;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SchemaUtil {

    public static void setSchema(Schema schema, ObjectNode info, String format) {

        switch (format.toLowerCase()) {
            case "json":
                getJsonSchema(schema, info);
                break;
            case "csv":
                getCsvSchema(schema, info);
                break;
            case "orc":
                getOrcSchema(schema, info);
                break;
            case "avro":
                getAvroSchema(schema, info);
                break;
            case "parquet":
                getParquetSchema(schema, info);
                break;
            default:
        }
    }

    public static FormatDescriptor setFormat(String format, Config config) throws Exception {
        FormatDescriptor formatDescriptor = null;
        switch (format.toLowerCase().trim()) {
            case "json":
                formatDescriptor = new Json().failOnMissingField(false).deriveSchema();
                break;
            case "csv":
                Csv csv = new Csv().deriveSchema();
                Field interPro = csv.getClass().getDeclaredField("internalProperties");
                interPro.setAccessible(true);
                Object desc = interPro.get(csv);
                Class<DescriptorProperties> descCls = DescriptorProperties.class;
                Method putMethod = descCls.getDeclaredMethod("put", String.class, String.class);
                putMethod.setAccessible(true);
                for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
                    String key = entry.getKey();
                    if (key.startsWith("format.") && !StringUtils.equals(key, "format.type")) {
                        String value = config.getString(key);
                        putMethod.invoke(desc, key, value);
                    }
                }
                formatDescriptor = csv;
                break;
            case "avro":
                formatDescriptor = new Avro().avroSchema(config.getString("schema"));
                break;
            case "orc":
            case "parquet":
            default:
                break;
        }
        return formatDescriptor;
    }

    private static void getJsonSchema(Schema schema, ObjectNode json) {
        Iterator<String> fieldIterator = json.fieldNames();
        while (fieldIterator.hasNext()) {
            String key = fieldIterator.next();
            JsonNode value = json.findValue(key);
            switch (value.getNodeType()) {
                case STRING:
                    schema.field(key, Types.STRING());
                    break;
                case NUMBER:
                    switch (value.numberType()) {
                        case INT:
                            schema.field(key, Types.INT());
                            break;
                        case LONG:
                            schema.field(key, Types.LONG());
                            break;
                        case BIG_DECIMAL:
                            schema.field(key, Types.JAVA_BIG_DEC());
                            break;
                        case DOUBLE:
                            schema.field(key, Types.DOUBLE());
                            break;
                        default:
                            break;
                    }
                    break;
                case ARRAY:
                    JsonNode jsonNode = value.get(0);
                    if (jsonNode.getNodeType().equals(JsonNodeType.OBJECT)) {
                        schema.field(key, ObjectArrayTypeInfo.getInfoFor(Row[].class, getTypeInformation((ObjectNode) jsonNode)));
                    } else {
                        schema.field(key, ObjectArrayTypeInfo.getInfoFor(Object[].class, TypeInformation.of(Object.class)));
                    }
                    break;
                case OBJECT:
                    schema.field(key, getTypeInformation(json));
                    break;
                default:
                    break;
            }
        }
    }

    private static void getCsvSchema(Schema schema, ObjectNode schemaList) {
        if (schemaList.isArray()) {
            for (JsonNode node : schemaList) {
                String field = node.get("field").asText();
                String type = node.get("type").asText().toUpperCase();
                schema.field(field, type);
            }
        }
    }

    public static TypeInformation[] getCsvType(List<Map<String, String>> schemaList) {
        TypeInformation[] typeInformation = new TypeInformation[schemaList.size()];
        int i = 0;
        for (Map<String, String> map : schemaList) {
            String type = map.get("type").toUpperCase();
            typeInformation[i++] = TypeStringUtils.readTypeInfo(type);
        }
        return typeInformation;
    }


    /**
     * todo
     *
     * @param schema schema
     * @param json   json
     */
    private static void getOrcSchema(Schema schema, ObjectNode json) {

    }


    /**
     * todo
     *
     * @param schema schema
     * @param json   json
     */
    private static void getParquetSchema(Schema schema, ObjectNode json) {

    }

    private static void getAvroSchema(Schema schema, ObjectNode json) {
        RowTypeInfo typeInfo = (RowTypeInfo) AvroSchemaConverter.<Row>convertToTypeInfo(json.toString());
        String[] fieldNames = typeInfo.getFieldNames();
        for (String name : fieldNames) {
            schema.field(name, typeInfo.getTypeAt(name));
        }
    }

    public static RowTypeInfo getTypeInformation(ObjectNode json) {
        int size = json.size();
        String[] fields = new String[size];
        TypeInformation[] informations = new TypeInformation[size];
        int i = 0;
        Iterator<String> fieldIterator = json.fieldNames();
        while (fieldIterator.hasNext()) {
            String key = fieldIterator.next();
            fields[i] = key;
            JsonNode value = json.findValue(key);
            switch (value.getNodeType()) {
                case OBJECT:
                    informations[i] = getTypeInformation((ObjectNode) value);
                    break;
                case ARRAY:
                    JsonNode jsonNode = value.get(0);
                    informations[i] = ObjectArrayTypeInfo.getInfoFor(Row[].class, getTypeInformation((ObjectNode) jsonNode));
                    break;
                case STRING:
                    informations[i] = Types.STRING();
                    break;
                case NUMBER:
                    switch (value.numberType()) {
                        case INT:
                            informations[i] = Types.INT();
                            break;
                        case LONG:
                            informations[i] = Types.LONG();
                            break;
                        case BIG_DECIMAL:
                            informations[i] = Types.JAVA_BIG_DEC();
                            break;
                        case DOUBLE:
                            informations[i] = Types.DOUBLE();
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
            i++;
        }
        return new RowTypeInfo(informations, fields);
    }

    public static String getUniqueTableName() {
        return UUID.randomUUID().toString().replaceAll("-", "_");
    }
}
