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

import org.apache.seatunnel.flink.enums.FormatType;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
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
import java.util.regex.Pattern;

public final class SchemaUtil {

    private static final Pattern DASH_COMPILE = Pattern.compile("-");

    private SchemaUtil() {
    }

    public static void setSchema(Schema schema, Object info, FormatType format) {

        switch (format) {
            case JSON:
                getJsonSchema(schema, (ObjectNode) info);
                break;
            case CSV:
                getCsvSchema(schema, (ArrayNode) info);
                break;
            case ORC:
                getOrcSchema(schema, (ObjectNode) info);
                break;
            case AVRO:
                getAvroSchema(schema, (ObjectNode) info);
                break;
            case PARQUET:
                getParquetSchema(schema, (ObjectNode) info);
                break;
            default:
        }
    }

    public static FormatDescriptor setFormat(FormatType format, Config config) throws Exception {
        FormatDescriptor formatDescriptor = null;
        switch (format) {
            case JSON:
                formatDescriptor = new Json().failOnMissingField(false).deriveSchema();
                break;
            case CSV:
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
            case AVRO:
                formatDescriptor = new Avro().avroSchema(config.getString("schema"));
                break;
            case ORC:
            case PARQUET:
            default:
                break;
        }
        return formatDescriptor;
    }

    private static void getJsonSchema(Schema schema, ObjectNode json) {
        Iterator<Map.Entry<String, JsonNode>> nodeIterator = json.fields();
        while (nodeIterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = nodeIterator.next();
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof TextNode) {
                schema.field(key, Types.STRING());
            } else if (value instanceof IntNode) {
                schema.field(key, Types.INT());
            } else if (value instanceof LongNode) {
                schema.field(key, Types.LONG());
            } else if (value instanceof DecimalNode) {
                schema.field(key, Types.JAVA_BIG_DEC());
            } else if (value instanceof FloatNode) {
                schema.field(key,  Types.FLOAT());
            } else if (value instanceof DoubleNode) {
                schema.field(key, Types.DOUBLE());
            } else if (value instanceof ObjectNode) {
                schema.field(key, getTypeInformation((ObjectNode) value));
            } else if (value instanceof ArrayNode) {
                Object obj = ((ArrayNode) value).get(0);
                if (obj instanceof ObjectNode) {
                    schema.field(key, ObjectArrayTypeInfo.getInfoFor(Row[].class, getTypeInformation((ObjectNode) obj)));
                } else {
                    schema.field(key, ObjectArrayTypeInfo.getInfoFor(Object[].class, TypeInformation.of(Object.class)));
                }
            }
        }
    }

    private static void getCsvSchema(Schema schema, ArrayNode schemaList) {
        Iterator<JsonNode> iterator = schemaList.elements();

        while (iterator.hasNext()) {
            JsonNode jsonNode = iterator.next();
            String field = jsonNode.get("field").textValue();
            String type = jsonNode.get("type").textValue().toUpperCase();

            schema.field(field, type);
        }
    }

    public static TypeInformation<?>[] getCsvType(List<Map<String, String>> schemaList) {
        TypeInformation<?>[] typeInformation = new TypeInformation[schemaList.size()];
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
        TypeInformation<?>[] informations = new TypeInformation[size];
        int i = 0;
        Iterator<Map.Entry<String, JsonNode>> nodeIterator = json.fields();
        while (nodeIterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = nodeIterator.next();
            String key = entry.getKey();
            Object value = entry.getValue();
            fields[i] = key;
            if (value instanceof TextNode) {
                informations[i] = Types.STRING();
            } else if (value instanceof IntNode) {
                informations[i] = Types.INT();
            } else if (value instanceof LongNode) {
                informations[i] = Types.LONG();
            } else if (value instanceof DecimalNode) {
                informations[i] = Types.JAVA_BIG_DEC();
            } else if (value instanceof FloatNode) {
                informations[i] = Types.FLOAT();
            } else if (value instanceof DoubleNode) {
                informations[i] = Types.DOUBLE();
            } else if (value instanceof ObjectNode) {
                informations[i] = getTypeInformation((ObjectNode) value);
            } else if (value instanceof ArrayNode) {
                ObjectNode demo = (ObjectNode) ((ArrayNode) value).get(0);
                informations[i] = ObjectArrayTypeInfo.getInfoFor(Row[].class, getTypeInformation(demo));
            }
            i++;
        }
        return new RowTypeInfo(informations, fields);
    }

    public static String getUniqueTableName() {
        return DASH_COMPILE.matcher(UUID.randomUUID().toString()).replaceAll("_");
    }
}
