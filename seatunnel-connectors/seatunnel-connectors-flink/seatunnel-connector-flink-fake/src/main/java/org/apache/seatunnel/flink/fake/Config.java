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

package org.apache.seatunnel.flink.fake;

import org.apache.seatunnel.flink.fake.source.FakeSource;
import org.apache.seatunnel.flink.fake.source.FakeSourceStream;

/**
 * FakeSource {@link FakeSource} and
 * FakeSourceStream {@link FakeSourceStream} configuration parameters
 */
public final class Config {

    /**
     * Configuration mock data schema in FakeSourceStream. It used when mock_data_enable = true
     */
    public static final String MOCK_DATA_SCHEMA = "mock_data_schema";

    /**
     * Each schema configuration 'name' param.
     * mock_data_schema = [
     * {
     * name = "colName"
     * type = "colType"
     * }
     * ]
     */
    public static final String MOCK_DATA_SCHEMA_NAME = "name";

    /**
     * Each schema configuration 'type' param.
     * Support Value:
     * int,integer,byte,boolean,char,character,short,long,float,double,date,timestamp,decimal,bigdecimal,bigint,biginteger,
     * int[],byte[],boolean[],char[],character[],short[],long[],float[],double[],string[],binary,varchar
     * mock_data_schema = [
     * {
     * name = "colName"
     * type = "colType"
     * },
     * ...
     * ]
     */
    public static final String MOCK_DATA_SCHEMA_TYPE = "type";

    /**
     * defined the rule of mock data.
     */
    public static final String MOCK_DATA_SCHEMA_MOCK = "mock_config";

    /**
     * sample1: 0,f
     * sample2: a,f
     * byte = Byte.parseByte(rangeMin).
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#byteRange(byte min, byte max)} .
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_BYTE_RANGE = "byte_range";

    /**
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#booleanSeed(boolean... seeds)} .
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_BOOLEAN_SEED = "boolean_seed";

    /**
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#charSeed(char... seeds)} .
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_CHAR_SEED = "char_seed";

    /**
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#dateRange(String min, String max)}  .
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_DATE_RANGE = "date_range";

    /**
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#decimalScale(int scale)} .
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_DECIMAL_SCALE = "decimal_scale";

    /**
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#doubleRange(double min, double max)}.
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_DOUBLE_RANGE = "double_range";

    /**
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#floatRange(float min, float max)}.
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_FLOAT_RANGE = "float_range";

    /**
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#intRange(int min, int max)} .
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_INT_RANGE = "int_range";

    /**
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#longRange(long min, long max)} .
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_LONG_RANGE = "long_range";

    /**
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#numberRegex(String regex)} .
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_NUMBER_REGEX = "number_regex";

    /**
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#timeRange(int minHour, int maxHour, int minMinute, int maxMinute, int minSecond, int maxSecond)} .
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_TIME_RANGE = "time_range";

    /**
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#sizeRange(int min, int max)} .
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_SIZE_RANGE = "size_range";

    /**
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#stringRegex(String regex)} .
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_STRING_REGEX = "string_regex";

    /**
     * defined the rule of mock data config {@link com.github.jsonzou.jmockdata.MockConfig#stringSeed(String... seeds)} .
     */
    public static final String MOCK_DATA_SCHEMA_MOCK_STRING_SEED = "string_seed";

    /**
     * In mock_data_schema=true, limit row size. Default is 300.
     */
    public static final String MOCK_DATA_SIZE = "mock_data_size";

    /**
     * mock_data_size Default value.
     */
    public static final int MOCK_DATA_SIZE_DEFAULT_VALUE = 300;

    /**
     * Create data interval, unit is second. Default is 1 second.
     */
    public static final String MOCK_DATA_INTERVAL = "mock_data_interval";

    /**
     * mock_data_interval Default value.
     */
    public static final long MOCK_DATA_INTERVAL_DEFAULT_VALUE = 1L;

}
