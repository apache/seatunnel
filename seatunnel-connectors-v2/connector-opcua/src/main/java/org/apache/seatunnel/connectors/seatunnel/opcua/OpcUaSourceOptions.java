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

package org.apache.seatunnel.connectors.seatunnel.opcua;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;

/** OPC UA data, endpoint, and resource settings. */
public final class OpcUaSourceOptions {
    public static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "OPC TCP endpoint advertised by the server, including its port and path.");
    public static final Option<String> READ_MODE =
            Options.key("read_mode")
                    .stringType()
                    .defaultValue("POLLING")
                    .withDescription("POLLING, SUBSCRIBE, or BROWSE_NODES.");
    public static final Option<String> NODE_FETCH_MODE =
            Options.key("node_fetch_mode")
                    .stringType()
                    .defaultValue("MANUAL")
                    .withDescription("MANUAL, FILE, or BROWSE node selection for value reads.");
    public static final Option<Map<String, String>> NODES =
            Options.key("nodes")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Manual aliases and OPC UA textual NodeIds.");
    public static final Option<String> NODE_FILE =
            Options.key("node_file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JSON file containing alias to textual NodeId entries.");
    public static final Option<String> BROWSE_ROOT =
            Options.key("browse_root")
                    .stringType()
                    .defaultValue("i=84")
                    .withDescription("Root textual NodeId for browsing.");
    public static final Option<Integer> BROWSE_PAGE_SIZE =
            Options.key("browse_page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum requested references per browse page.");
    public static final Option<Integer> MAX_NODES =
            Options.key("max_nodes")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Maximum distinct nodes in one browse or explicit selection.");
    public static final Option<Integer> MAX_DEPTH =
            Options.key("max_depth")
                    .intType()
                    .defaultValue(16)
                    .withDescription("Maximum depth below the browse root.");
    public static final Option<Integer> INTERVAL_MS =
            Options.key("interval_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Polling cadence or subscription publishing and sampling interval.");
    public static final Option<Integer> REFRESH_INTERVAL_MS =
            Options.key("refresh_interval_ms")
                    .intType()
                    .defaultValue(3600000)
                    .withDescription("Refresh period for browse-selected value nodes.");
    public static final Option<Integer> READ_BATCH_SIZE =
            Options.key("read_batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Maximum number of values or monitored items in one service call.");
    public static final Option<Integer> QUEUE_CAPACITY =
            Options.key("queue_capacity")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Maximum queued subscription samples per reader.");
    public static final Option<Long> MAX_RECORDS =
            Options.key("max_records")
                    .longType()
                    .defaultValue(0L)
                    .withDescription(
                            "Maximum records for a bounded run. Zero means no row-count limit.");
    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(5000)
                    .withDescription("Deadline for one network operation or complete browse.");
    public static final Option<Integer> SESSION_TIMEOUT_MS =
            Options.key("session_timeout_ms")
                    .intType()
                    .defaultValue(120000)
                    .withDescription("Requested OPC UA session timeout and reconnection budget.");
    public static final Option<String> SECURITY_POLICY =
            Options.key("security_policy")
                    .stringType()
                    .defaultValue("Basic256Sha256")
                    .withDescription("None, Basic128Rsa15, Basic256, or Basic256Sha256.");
    public static final Option<String> MESSAGE_SECURITY_MODE =
            Options.key("message_security_mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("None, Sign, or SignAndEncrypt.");
    public static final Option<Boolean> ALLOW_INSECURE =
            Options.key("allow_insecure")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Permit an explicitly chosen None endpoint.");
    public static final Option<Boolean> ALLOW_LEGACY_SECURITY =
            Options.key("allow_legacy_security")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Permit a legacy Basic128Rsa15 or Basic256 endpoint.");
    public static final Option<String> APPLICATION_URI =
            Options.key("application_uri")
                    .stringType()
                    .defaultValue("urn:apache:seatunnel:opcua:client")
                    .withDescription(
                            "Client application URI, also present in the client certificate SAN.");
    public static final Option<String> SERVER_APPLICATION_URI =
            Options.key("server_application_uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Expected server application URI for a secured endpoint.");
    public static final Option<String> SERVER_CERTIFICATE_SHA256 =
            Options.key("server_certificate_sha256")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "SHA-256 fingerprint of the trusted server application certificate.");
    public static final Option<String> CLIENT_KEY_STORE =
            Options.key("client_key_store")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("PKCS12 client key store on the worker.");
    public static final Option<String> CLIENT_KEY_ALIAS =
            Options.key("client_key_alias")
                    .stringType()
                    .defaultValue("client")
                    .withDescription("Client private key alias in the PKCS12 key store.");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password of the client PKCS12 key store and key.");
    public static final Option<Integer> MAX_CHUNK_SIZE =
            Options.key("max_chunk_size")
                    .intType()
                    .defaultValue(65536)
                    .withDescription("Maximum channel chunk size in bytes.");
    public static final Option<Integer> MAX_CHUNK_COUNT =
            Options.key("max_chunk_count")
                    .intType()
                    .defaultValue(32)
                    .withDescription("Maximum number of chunks in one channel message.");
    public static final Option<Integer> MAX_MESSAGE_SIZE =
            Options.key("max_message_size")
                    .intType()
                    .defaultValue(2097152)
                    .withDescription("Maximum incoming OPC UA message bytes.");
    public static final Option<Integer> MAX_ARRAY_LENGTH =
            Options.key("max_array_length")
                    .intType()
                    .defaultValue(65536)
                    .withDescription("Maximum decoded value array elements.");
    public static final Option<Integer> MAX_STRING_BYTES =
            Options.key("max_string_bytes")
                    .intType()
                    .defaultValue(65536)
                    .withDescription("Maximum individual normalized string or ByteString length.");

    private OpcUaSourceOptions() {}
}
