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

package org.apache.seatunnel.connectors.seatunnel.coap;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.Map;

/** Options shared by the CoAP transport and its direction-specific factories. */
final class CoapOptions {
    static final String IDENTIFIER = "CoAP";
    static final Option<String> BIND_HOST =
            Options.key("bind_host")
                    .stringType()
                    .defaultValue("0.0.0.0")
                    .withDescription("Numeric address of the UDP listener.");
    static final Option<Integer> PORT =
            Options.key("port").intType().defaultValue(5683).withDescription("UDP listener port.");
    static final Option<String> RESOURCE =
            Options.key("resource_path")
                    .stringType()
                    .defaultValue("/sdc")
                    .withDescription("Absolute path of the CoAP listener resource.");
    static final Option<String> URI =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Absolute unicast coap URI of the receiver.");
    static final Option<Format> FORMAT =
            Options.key("format")
                    .enumType(Format.class)
                    .defaultValue(Format.JSON)
                    .withDescription("Message payload format.");
    static final Option<Method> METHOD =
            Options.key("method")
                    .enumType(Method.class)
                    .defaultValue(Method.POST)
                    .withDescription("Method used for each input row.");
    static final Option<RequestType> REQUEST_TYPE =
            Options.key("request_type")
                    .enumType(RequestType.class)
                    .defaultValue(RequestType.NON)
                    .withDescription("Confirmable CON or non-confirmable NON request.");
    static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(5000)
                    .withDescription("Maximum time to wait for a response for one row.");
    static final Option<Integer> BODY_SIZE =
            Options.key("max_payload_bytes")
                    .intType()
                    .defaultValue(65536)
                    .withDescription("Maximum assembled payload size, in bytes.");
    static final Option<Integer> QUEUE =
            Options.key("queue_capacity")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "Maximum complete inbound messages waiting for the source reader.");
    static final Option<Integer> PEERS =
            Options.key("max_peers")
                    .intType()
                    .defaultValue(64)
                    .withDescription("Maximum simultaneously tracked source peer addresses.");
    static final Option<Integer> CONCURRENCY =
            Options.key("max_concurrent_requests")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum source protocol workers.");
    static final Option<Integer> BLOCK_TIMEOUT =
            Options.key("blockwise_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Inactivity limit for a partial blockwise transfer.");
    static final Option<Long> LIMIT =
            Options.key("stop_after_messages")
                    .longType()
                    .defaultValue(0L)
                    .withDescription(
                            "Stop after this many accepted messages, or zero for continuous collection.");
    static final Option<String> PAYLOAD_FIELD =
            Options.key("payload_field")
                    .stringType()
                    .defaultValue("payload")
                    .withDescription("Input STRING or BYTES field for text or binary payloads.");
    static final Option<String> CSV_DELIMITER =
            Options.key("csv_delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("Single-character CSV delimiter.");
    static final Option<String> AVRO_SCHEMA =
            Options.key("avro_writer_schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional writer schema for a single raw Avro record.");
    static final Option<String> PROTOBUF_SCHEMA =
            Options.key("protobuf_schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Protocol Buffers schema for this message.");
    static final Option<String> PROTOBUF_MESSAGE =
            Options.key("protobuf_message_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Fully qualified Protocol Buffers message name.");

    static final Option<String> XML_PATH =
            Options.key("xml_record_path")
                    .stringType()
                    .defaultValue("/*")
                    .withDescription("XPath selecting XML record elements.");
    static final Option<Map<String, String>> XML_NAMESPACES =
            Options.key("xml_namespaces")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription("Namespace URIs by XPath prefix.");
    static final Option<String> RECORD_HEADER_FIELD =
            Options.key("record_header_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional typed output field retaining the record envelope header.");
    static final Option<Boolean> XML_PRESERVE_ROOT =
            Options.key("xml_preserve_root")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Keep the selected XML element name around its fields.");
    static final Option<LogMode> LOG_MODE =
            Options.key("log_mode")
                    .enumType(LogMode.class)
                    .defaultValue(LogMode.COMMON_LOG_FORMAT)
                    .withDescription("Log record syntax.");
    static final Option<String> LOG_PATTERN =
            Options.key("log_pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Configured regex, Grok pattern or log layout.");
    static final Option<Map<String, Integer>> LOG_GROUPS =
            Options.key("log_groups")
                    .type(new TypeReference<Map<String, Integer>>() {})
                    .defaultValue(Collections.emptyMap())
                    .withDescription("Regex capture indexes by output field.");
    static final Option<Map<String, String>> GROK_DEFINITIONS =
            Options.key("grok_definitions")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription("Additional Grok regexes by pattern name.");
    static final Option<DatagramMode> DATAGRAM_MODE =
            Options.key("datagram_mode")
                    .enumType(DatagramMode.class)
                    .defaultValue(DatagramMode.RAW_DATA)
                    .withDescription("Protocol carried inside CoAP datagram bodies.");
    static final Option<String> DATAGRAM_SEPARATOR =
            Options.key("datagram_separator")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional literal UTF-8 separator for raw datagram records.");
    static final Option<Integer> NETFLOW_TEMPLATES =
            Options.key("netflow_max_templates")
                    .intType()
                    .defaultValue(128)
                    .withDescription("Maximum retained NetFlow v9 templates.");
    static final Option<String> COLLECTD_USER =
            Options.key("collectd_username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional username for authenticated collectd packets.");
    static final Option<String> COLLECTD_PASSWORD =
            Options.key("collectd_password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional password for authenticated collectd packets.");
    static final Option<Integer> NETFLOW_TEMPLATE_TIMEOUT =
            Options.key("netflow_template_timeout_ms")
                    .intType()
                    .defaultValue(300000)
                    .withDescription("NetFlow v9 template lifetime.");
    static final Option<Map<String, String>> COLLECTD_TYPES =
            Options.key("collectd_types")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription("Optional comma-separated metric names by collectd type.");
    static final Option<Boolean> COLLECTD_CONVERT_TIME =
            Options.key("collectd_convert_time")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Normalize collectd time and interval values to milliseconds.");
    static final Option<Boolean> COLLECTD_OMIT_INTERVAL =
            Options.key("collectd_omit_interval")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Omit collectd interval values from emitted rows.");
    static final Option<Boolean> AVRO_CONTAINER =
            Options.key("avro_container")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Write one Avro object container per row.");

    /** Message representations supported by one or both connector directions. */
    enum Format {
        JSON,
        TEXT,
        BINARY,
        CSV,
        AVRO,
        PROTOBUF,
        XML,
        LOG,
        DATAGRAM,
        RECORD_JSON
    }

    /** Built-in parser layouts for application and security logs. */
    enum LogMode {
        COMMON_LOG_FORMAT,
        COMBINED_LOG_FORMAT,
        APACHE_ERROR_LOG_FORMAT,
        APACHE_CUSTOM_LOG_FORMAT,
        REGEX,
        GROK,
        LOG4J,
        CEF,
        LEEF
    }

    /** Interpretation of a datagram enclosed in a complete CoAP message. */
    enum DatagramMode {
        RAW_DATA,
        SYSLOG,
        COLLECTD,
        NETFLOW
    }

    /** Methods a sink can send once for each input row. */
    enum Method {
        GET,
        POST,
        PUT,
        DELETE
    }

    /** CoAP transport confirmation type of an outgoing request. */
    enum RequestType {
        CON,
        NON
    }

    private CoapOptions() {}
}
