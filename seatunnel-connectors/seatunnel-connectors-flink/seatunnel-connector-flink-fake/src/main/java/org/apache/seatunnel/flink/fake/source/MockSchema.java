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

package org.apache.seatunnel.flink.fake.source;

import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_BOOLEAN_SEED;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_BYTE_RANGE;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_CHAR_SEED;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_DATE_RANGE;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_DECIMAL_SCALE;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_DOUBLE_RANGE;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_FLOAT_RANGE;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_INT_RANGE;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_LONG_RANGE;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_NUMBER_REGEX;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_SIZE_RANGE;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_STRING_REGEX;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_STRING_SEED;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK_TIME_RANGE;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_NAME;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_TYPE;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import com.github.jsonzou.jmockdata.JMockData;
import com.github.jsonzou.jmockdata.MockConfig;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * config param [mock_data_schema] json bean.
 * @see org.apache.seatunnel.flink.fake.Config#MOCK_DATA_SCHEMA
 */
public class MockSchema implements Serializable {

    private static final long serialVersionUID = -7018198671355055858L;

    private static final int STATIC_RANGE_SIZE = 2;
    private static final int STATIC_RANGE_MIN_INDEX = 0;
    private static final int STATIC_RANGE_MAX_INDEX = 1;
    private static final int TIME_RANGE_SIZE = 6;
    private static final int TIME_RANGE_HOUR_MIN_INDEX = 0;
    private static final int TIME_RANGE_HOUR_MAX_INDEX = 1;
    private static final int TIME_RANGE_MINUTE_MIN_INDEX = 2;
    private static final int TIME_RANGE_MINUTE_MAX_INDEX = 3;
    private static final int TIME_RANGE_SECOND_MIN_INDEX = 4;
    private static final int TIME_RANGE_SECOND_MAX_INDEX = 5;

    private String name;
    private String type;
    private Config mockConfig;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Config getMockConfig() {
        return mockConfig;
    }

    public void setMockConfig(Config mockConfig) {
        this.mockConfig = mockConfig;
    }

    public TypeInformation<?> typeInformation() {
        TypeInformation<?> dataType;
        switch (this.type.trim().toLowerCase()) {
            case "int":
            case "integer":
                dataType = BasicTypeInfo.INT_TYPE_INFO;
                break;
            case "byte":
                dataType = BasicTypeInfo.BYTE_TYPE_INFO;
                break;
            case "boolean":
                dataType = BasicTypeInfo.BOOLEAN_TYPE_INFO;
                break;
            case "char":
            case "character":
                dataType = BasicTypeInfo.CHAR_TYPE_INFO;
                break;
            case "short":
                dataType = BasicTypeInfo.SHORT_TYPE_INFO;
                break;
            case "long":
                dataType = BasicTypeInfo.LONG_TYPE_INFO;
                break;
            case "float":
                dataType = BasicTypeInfo.FLOAT_TYPE_INFO;
                break;
            case "double":
                dataType = BasicTypeInfo.DOUBLE_TYPE_INFO;
                break;
            case "date":
                dataType = BasicTypeInfo.DATE_TYPE_INFO;
                break;
            case "timestamp":
                dataType = SqlTimeTypeInfo.TIMESTAMP;
                break;
            case "decimal":
            case "bigdecimal":
                dataType = BasicTypeInfo.BIG_DEC_TYPE_INFO;
                break;
            case "bigint":
            case "biginteger":
                dataType = BasicTypeInfo.BIG_INT_TYPE_INFO;
                break;
            case "int[]":
                dataType = PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO;
                break;
            case "byte[]":
                dataType = PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
                break;
            case "boolean[]":
                dataType = PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO;
                break;
            case "char[]":
            case "character[]":
                dataType = PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO;
                break;
            case "short[]":
                dataType = PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO;
                break;
            case "long[]":
                dataType = PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO;
                break;
            case "float[]":
                dataType = PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO;
                break;
            case "double[]":
                dataType = PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO;
                break;
            case "string[]":
                dataType = BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
                break;
            case "binary":
                dataType = GenericTypeInfo.of(ByteArrayInputStream.class);
                break;
            case "varchar":
            default:
                dataType = BasicTypeInfo.STRING_TYPE_INFO;
                break;
        }
        return dataType;
    }

    public Object mockData(){
        Object mockData;
        MockConfig mockConfig = new MockConfig();
        resolve(mockConfig);
        switch (this.type.trim().toLowerCase()){
            case "int":
            case "integer":
                mockData = JMockData.mock(int.class, mockConfig);
                break;
            case "byte":
                mockData = JMockData.mock(byte.class, mockConfig);
                break;
            case "boolean":
                mockData = JMockData.mock(boolean.class, mockConfig);
                break;
            case "char":
            case "character":
                mockData = JMockData.mock(char.class, mockConfig);
                break;
            case "short":
                mockData = JMockData.mock(short.class, mockConfig);
                break;
            case "long":
                mockData = JMockData.mock(long.class, mockConfig);
                break;
            case "float":
                mockData = JMockData.mock(float.class, mockConfig);
                break;
            case "double":
                mockData = JMockData.mock(double.class, mockConfig);
                break;
            case "date":
                mockData = JMockData.mock(Date.class, mockConfig);
                break;
            case "timestamp":
                mockData = JMockData.mock(Timestamp.class, mockConfig);
                break;
            case "decimal":
            case "bigdecimal":
                mockData = JMockData.mock(BigDecimal.class, mockConfig);
                break;
            case "bigint":
            case "biginteger":
                mockData = JMockData.mock(BigInteger.class, mockConfig);
                break;
            case "int[]":
                mockData = JMockData.mock(int[].class, mockConfig);
                break;
            case "byte[]":
                mockData = JMockData.mock(byte[].class, mockConfig);
                break;
            case "boolean[]":
                mockData = JMockData.mock(boolean[].class, mockConfig);
                break;
            case "char[]":
            case "character[]":
                mockData = JMockData.mock(char[].class, mockConfig);
                break;
            case "short[]":
                mockData = JMockData.mock(short[].class, mockConfig);
                break;
            case "long[]":
                mockData = JMockData.mock(long[].class, mockConfig);
                break;
            case "float[]":
                mockData = JMockData.mock(float[].class, mockConfig);
                break;
            case "double[]":
                mockData = JMockData.mock(double[].class, mockConfig);
                break;
            case "string[]":
                mockData = JMockData.mock(String[].class, mockConfig);
                break;
            case "binary":
                byte[] bytes = JMockData.mock(byte[].class, mockConfig);
                mockData = new ByteArrayInputStream(bytes);
                break;
            case "varchar":
            default:
                mockData = JMockData.mock(String.class, mockConfig);
                break;
        }
        return mockData;
    }

    private void resolve(MockConfig mockConfig) {
        if (this.mockConfig != null) {
            byteConfigResolve(mockConfig);
            booleanConfigResolve(mockConfig);
            charConfigResolve(mockConfig);
            dateConfigResolve(mockConfig);
            decimalConfigResolve(mockConfig);
            doubleConfigResolve(mockConfig);
            floatConfigResolve(mockConfig);
            intConfigResolve(mockConfig);
            longConfigResolve(mockConfig);
            numberConfigResolve(mockConfig);
            sizeConfigResolve(mockConfig);
            stringConfigResolve(mockConfig);
            timeConfigResolve(mockConfig);
        }
    }

    private void byteConfigResolve(MockConfig mockConfig) {
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_BYTE_RANGE)) {
            List<Long> byteRange = this.mockConfig.getBytesList(MOCK_DATA_SCHEMA_MOCK_BYTE_RANGE);
            assert byteRange.size() >= STATIC_RANGE_SIZE;
            mockConfig.byteRange(byteRange.get(STATIC_RANGE_MIN_INDEX).byteValue(), byteRange.get(STATIC_RANGE_MAX_INDEX).byteValue());
        }
    }

    private void booleanConfigResolve(MockConfig mockConfig) {
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_BOOLEAN_SEED)) {
            List<Boolean> booleanSeedList = this.mockConfig.getBooleanList(MOCK_DATA_SCHEMA_MOCK_BOOLEAN_SEED);
            boolean[] booleanSeed = new boolean[booleanSeedList.size()];
            for (int index = 0; index < booleanSeedList.size(); index++) {
                booleanSeed[index] = booleanSeedList.get(index);
            }
            mockConfig.booleanSeed(booleanSeed);
        }
    }

    private void charConfigResolve(MockConfig mockConfig) {
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_CHAR_SEED)) {
            String charSeedString = this.mockConfig.getString(MOCK_DATA_SCHEMA_MOCK_CHAR_SEED);
            byte[] bytes = charSeedString.getBytes(StandardCharsets.UTF_8);
            char[] charSeed = new char[bytes.length];
            for (int index = 0; index < bytes.length; index++) {
                charSeed[index] = (char) bytes[index];
            }
            mockConfig.charSeed(charSeed);
        }
    }

    private void dateConfigResolve(MockConfig mockConfig) {
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_DATE_RANGE)) {
            List<String> dateRange = this.mockConfig.getStringList(MOCK_DATA_SCHEMA_MOCK_DATE_RANGE);
            assert dateRange.size() >= STATIC_RANGE_SIZE;
            mockConfig.dateRange(dateRange.get(STATIC_RANGE_MIN_INDEX), dateRange.get(STATIC_RANGE_MAX_INDEX));
        }
    }

    private void decimalConfigResolve(MockConfig mockConfig) {
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_DECIMAL_SCALE)) {
            int scala = this.mockConfig.getInt(MOCK_DATA_SCHEMA_MOCK_DECIMAL_SCALE);
            mockConfig.decimalScale(scala);
        }
    }

    private void doubleConfigResolve(MockConfig mockConfig) {
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_DOUBLE_RANGE)) {
            List<Double> doubleRange = this.mockConfig.getDoubleList(MOCK_DATA_SCHEMA_MOCK_DOUBLE_RANGE);
            assert doubleRange.size() >= STATIC_RANGE_SIZE;
            mockConfig.doubleRange(doubleRange.get(STATIC_RANGE_MIN_INDEX), doubleRange.get(STATIC_RANGE_MAX_INDEX));
        }
    }

    private void floatConfigResolve(MockConfig mockConfig) {
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_FLOAT_RANGE)) {
            List<Double> floatRange = this.mockConfig.getDoubleList(MOCK_DATA_SCHEMA_MOCK_FLOAT_RANGE);
            assert floatRange.size() >= STATIC_RANGE_SIZE;
            mockConfig.floatRange(floatRange.get(STATIC_RANGE_MIN_INDEX).floatValue(), floatRange.get(STATIC_RANGE_MAX_INDEX).floatValue());
        }
    }

    private void intConfigResolve(MockConfig mockConfig) {
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_INT_RANGE)) {
            List<Integer> intRange = this.mockConfig.getIntList(MOCK_DATA_SCHEMA_MOCK_INT_RANGE);
            assert intRange.size() >= STATIC_RANGE_SIZE;
            mockConfig.intRange(intRange.get(STATIC_RANGE_MIN_INDEX), intRange.get(STATIC_RANGE_MAX_INDEX));
        }
    }

    private void longConfigResolve(MockConfig mockConfig) {
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_LONG_RANGE)) {
            List<Long> longRange = this.mockConfig.getLongList(MOCK_DATA_SCHEMA_MOCK_LONG_RANGE);
            assert longRange.size() >= STATIC_RANGE_SIZE;
            mockConfig.longRange(longRange.get(STATIC_RANGE_MIN_INDEX), longRange.get(STATIC_RANGE_MAX_INDEX));
        }
    }

    private void numberConfigResolve(MockConfig mockConfig) {
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_NUMBER_REGEX)) {
            String numberRegex = this.mockConfig.getString(MOCK_DATA_SCHEMA_MOCK_NUMBER_REGEX);
            mockConfig.numberRegex(numberRegex);
        }
    }

    private void sizeConfigResolve(MockConfig mockConfig) {
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_SIZE_RANGE)) {
            List<Integer> sizeRange = this.mockConfig.getIntList(MOCK_DATA_SCHEMA_MOCK_SIZE_RANGE);
            assert sizeRange.size() >= STATIC_RANGE_SIZE;
            mockConfig.sizeRange(sizeRange.get(STATIC_RANGE_MIN_INDEX), sizeRange.get(STATIC_RANGE_MAX_INDEX));
        }
    }

    private void stringConfigResolve(MockConfig mockConfig) {
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_STRING_REGEX)) {
            String stringRegex = this.mockConfig.getString(MOCK_DATA_SCHEMA_MOCK_STRING_REGEX);
            mockConfig.stringRegex(stringRegex);
        }
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_STRING_SEED)) {
            List<String> stringSeed = this.mockConfig.getStringList(MOCK_DATA_SCHEMA_MOCK_STRING_SEED);
            mockConfig.stringSeed(stringSeed.toArray(new String[0]));
        }
    }

    private void timeConfigResolve(MockConfig mockConfig) {
        if (this.mockConfig.hasPath(MOCK_DATA_SCHEMA_MOCK_TIME_RANGE)) {
            List<Integer> timeRange = this.mockConfig.getIntList(MOCK_DATA_SCHEMA_MOCK_TIME_RANGE);
            assert timeRange.size() >= TIME_RANGE_SIZE;
            mockConfig.timeRange(
                timeRange.get(TIME_RANGE_HOUR_MIN_INDEX),
                timeRange.get(TIME_RANGE_HOUR_MAX_INDEX),
                timeRange.get(TIME_RANGE_MINUTE_MIN_INDEX),
                timeRange.get(TIME_RANGE_MINUTE_MAX_INDEX),
                timeRange.get(TIME_RANGE_SECOND_MIN_INDEX),
                timeRange.get(TIME_RANGE_SECOND_MAX_INDEX)
            );
        }
    }

    public static RowTypeInfo mockRowTypeInfo(List<MockSchema> mockDataSchema) {
        TypeInformation<?>[] types = new TypeInformation[mockDataSchema.size()];
        String[] fieldNames = new String[mockDataSchema.size()];
        for (int index = 0; index < mockDataSchema.size(); index++) {
            MockSchema schema = mockDataSchema.get(index);
            types[index] = schema.typeInformation();
            fieldNames[index] = schema.getName();
        }
        return new RowTypeInfo(types, fieldNames);
    }

    public static Row mockRowData(List<MockSchema> mockDataSchema){
        Object[] fieldByPosition = new Object[mockDataSchema.size()];
        for (int index = 0; index < mockDataSchema.size(); index++) {
            MockSchema schema = mockDataSchema.get(index);
            Object colData = schema.mockData();
            fieldByPosition[index] = colData;
        }
        return Row.of(fieldByPosition);
    }

    private static final String[] NAME_SEEDS = new String[]{"Gary", "Ricky Huo", "Kid Xiong"};

    private static final Integer[] NAME_SIZE_RANGE = new Integer[]{1, 1};

    private static final Integer[] AGE_RANGE = new Integer[]{1, 100};

    public static List<MockSchema> DEFAULT_MOCK_SCHEMAS = new ArrayList<>(0);

    static {
        MockSchema nameSchema = new MockSchema();
        nameSchema.setName("name");
        nameSchema.setType("string");
        Map<String, Object> nameSchemaConfigMap = new HashMap<>(0);
        nameSchemaConfigMap.put(MOCK_DATA_SCHEMA_MOCK_STRING_SEED, Arrays.asList(NAME_SEEDS));
        nameSchemaConfigMap.put(MOCK_DATA_SCHEMA_MOCK_SIZE_RANGE, Arrays.asList(NAME_SIZE_RANGE));
        nameSchema.setMockConfig(
            ConfigFactory.parseMap(
                nameSchemaConfigMap
            )
        );
        DEFAULT_MOCK_SCHEMAS.add(nameSchema);
        MockSchema ageSchema = new MockSchema();
        ageSchema.setName("age");
        ageSchema.setType("int");
        Map<String, Object> ageSchemaConfigMap = new HashMap<>(0);
        ageSchemaConfigMap.put(MOCK_DATA_SCHEMA_MOCK_INT_RANGE, Arrays.asList(AGE_RANGE));
        ageSchema.setMockConfig(
            ConfigFactory.parseMap(
                ageSchemaConfigMap
            )
        );
        DEFAULT_MOCK_SCHEMAS.add(ageSchema);
    }

    public static List<MockSchema> resolveConfig(Config config){
        if (config.hasPath(MOCK_DATA_SCHEMA)) {
            return config.getConfigList(MOCK_DATA_SCHEMA)
                .stream()
                .map(
                    schemaConfig -> {
                        MockSchema schema = new MockSchema();
                        schema.setName(schemaConfig.getString(MOCK_DATA_SCHEMA_NAME));
                        schema.setType(schemaConfig.getString(MOCK_DATA_SCHEMA_TYPE));
                        if (schemaConfig.hasPath(MOCK_DATA_SCHEMA_MOCK)) {
                            schema.setMockConfig(schemaConfig.getConfig(MOCK_DATA_SCHEMA_MOCK));
                        }
                        return schema;
                    }
                )
                .collect(Collectors.toList());
        }
        return DEFAULT_MOCK_SCHEMAS;
    }
}
