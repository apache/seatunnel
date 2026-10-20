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

package org.apache.seatunnel.connectors.seatunnel.natsjetstream.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NatsJetStreamSinkOptions {
    public static final String CONNECTOR_IDENTITY = "NatsJetStream";

    public static final String NATIVE_MAPPING_ID = "id";
    public static final String NATIVE_MAPPING_SUBJECT = "subject";
    public static final String NATIVE_MAPPING_HEADERS = "headers";
    public static final String NATIVE_MAPPING_DATA = "data";

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "NATS server address used by the sink, for example `nats://127.0.0.1:4222`."
                                    + " Keep credentials out of the URL and configure `username`/`password` or `token` separately.");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Username for NATS authentication. This option must be configured together with `password`"
                                    + " and must not be combined with `token`.");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password for NATS authentication. This option must be configured together with `username`"
                                    + " and must not be combined with `token`.");

    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Authentication token for NATS. Configure either `token` or the `username`/`password` pair.");

    public static final Option<String> SUBJECT =
            Options.key("subject")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Default JetStream subject used for published messages. This option is required in `json` format"
                                    + " and is used as the fallback subject in `native` format when the mapped subject field is null.");

    public static final Option<NatsJetStreamMessageFormat> FORMAT =
            Options.key("format")
                    .enumType(NatsJetStreamMessageFormat.class)
                    .defaultValue(NatsJetStreamMessageFormat.JSON)
                    .withDescription(
                            "Message format written to JetStream. Supported values are `json` and `native`.");

    public static final Option<Map<String, String>> NATIVE_FIELDS =
            Options.key("native_format_fields")
                    .mapType()
                    .defaultValue(getDefaultNativeFields())
                    .withDescription(
                            "Field mapping used only when `format = native`. Supported mapping keys are `id`, `subject`, `headers`, and `data`."
                                    + " Values are SeaTunnel field names. The default mapping is "
                                    + "`{\"id\":\"id\",\"subject\":\"subject\",\"headers\":\"headers\",\"data\":\"data\"}`."
                                    + " Optional mappings (`id`, `subject`, `headers`) are silently skipped when the mapped field"
                                    + " does not exist in the input schema. `data` must map to a BYTES field,"
                                    + " `subject` and `id` must map to STRING fields when present,"
                                    + " and `headers` must map to MAP<STRING, STRING> when present.");

    public static final Option<Boolean> INCLUDE_ROW_KIND_HEADER =
            Options.key("include_row_kind_header")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to add JetStream header `x-seatunnel-row-kind` in `native` format."
                                    + " When enabled, the header value is the SeaTunnel row kind name."
                                    + " This option has no effect in `json` format.");

    public static Map<String, String> getDefaultNativeFields() {
        Map<String, String> map = new HashMap<>();
        map.put(NATIVE_MAPPING_ID, NATIVE_MAPPING_ID);
        map.put(NATIVE_MAPPING_SUBJECT, NATIVE_MAPPING_SUBJECT);
        map.put(NATIVE_MAPPING_HEADERS, NATIVE_MAPPING_HEADERS);
        map.put(NATIVE_MAPPING_DATA, NATIVE_MAPPING_DATA);
        return Collections.unmodifiableMap(map);
    }
}
