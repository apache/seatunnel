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

package org.apache.seatunnel.flink.kafka;

/**
 * Kafka sink {@link org.apache.seatunnel.flink.kafka.sink.KafkaSink} configuration parameters
 */
public final class Config {

    /**
     * serialization type
     */
    public static final String KAFKA_SINK_FORMAT_TYPE = "format.type";

    /**
     * Field delimiter character for csv
     */
    public static final String KAFKA_SINK_FORMAT_CSV_FIELD_DELIMITER = "format.field_delimiter";

    /**
     * Line delimiter character for csv
     */
    public static final String KAFKA_SINK_FORMAT_CSV_LINE_DELIMITER = "format.line_delimiter";

    /**
     * Disabled quote character for enclosing field values
     */
    public static final String KAFKA_SINK_FORMAT_CSV_DISABLE_QUOTE_CHARACTER = "format.disable_quote_character";

    /**
     * Quote character for enclosing field values
     */
    public static final String KAFKA_SINK_FORMAT_CSV_QUOTE_CHARACTER = "format.quote_character";

    /**
     * Array element delimiter string for separating array and row element values
     */
    public static final String KAFKA_SINK_FORMAT_CSV_ARRAY_ELEMENT_DELIMITER = "format.array_element_delimiter";

    /**
     * Escape character for escaping values
     */
    public static final String KAFKA_SINK_FORMAT_CSV_ESCAPE_CHARACTER = "format.escape_character";

    /**
     * Null literal string that is interpreted as a null value
     */
    public static final String KAFKA_SINK_FORMAT_CSV_NULL_LITERAL = "format.null_literal";
}
