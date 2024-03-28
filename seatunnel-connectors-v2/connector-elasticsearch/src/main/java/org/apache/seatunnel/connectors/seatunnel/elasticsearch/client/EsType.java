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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.client;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@Getter
@AllArgsConstructor
public class EsType {

    public static final String AGGREGATE_METRIC_DOUBLE = "aggregate_metric_double";
    public static final String ALIAS = "alias";
    public static final String BINARY = "binary";
    public static final String BYTE = "byte";
    public static final String BOOLEAN = "boolean";
    public static final String COMPLETION = "completion";
    public static final String DATE = "date";
    public static final String DATE_NANOS = "date_nanos";
    public static final String DENSE_VECTOR = "dense_vector";
    public static final String DOUBLE = "double";
    public static final String FLATTENED = "flattened";
    public static final String FLOAT = "float";
    public static final String GEO_POINT = "geo_point";
    public static final String GEO_SHAPE = "geo_shape";
    public static final String POINT = "point";
    public static final String INTEGER_RANGE = "integer_range";
    public static final String FLOAT_RANGE = "float_range";
    public static final String LONG_RANGE = "long_range";
    public static final String DOUBLE_RANGE = "double_range";
    public static final String DATE_RANGE = "date_range";
    public static final String IP_RANGE = "ip_range";
    public static final String HALF_FLOAT = "half_float";
    public static final String SCALED_FLOAT = "scaled_float";
    public static final String HISTOGRAM = "histogram";
    public static final String INTEGER = "integer";
    public static final String IP = "ip";
    public static final String JOIN = "join";
    public static final String KEYWORD = "keyword";
    public static final String LONG = "long";
    public static final String NESTED = "nested";
    public static final String OBJECT = "object";
    public static final String PERCOLATOR = "percolator";
    public static final String RANK_FEATURE = "rank_feature";
    public static final String RANK_FEATURES = "rank_features";
    public static final String SEARCH_AS_YOU_TYPE = "search_as_you_type";
    public static final String SHORT = "short";
    public static final String SHAPE = "shape";
    public static final String STRING = "string";
    public static final String SPARSE_VECTOR = "sparse_vector";
    public static final String TEXT = "text";
    public static final String MATCH_ONLY_TEXT = "match_only_text";
    public static final String TOKEN_COUNT = "token_count";
    public static final String UNSIGNED_LONG = "unsigned_long";
    public static final String VERSION = "version";

    private String type;
    private Map<String, Object> options;
}
