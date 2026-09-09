/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.seatunnel.connectors.seatunnel.mem0.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions;

public class Mem0Options extends HttpCommonOptions {
    public static final String DEFAULT_API_BASE_URL = "https://api.mem0.ai";
    public static final String ADD_PATH = "/v3/memories/add/";
    public static final String AUTHORIZATION = "Authorization";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String ACCEPT = "Accept";
    public static final String APPLICATION_JSON = "application/json";

    public static final Option<String> API_BASE_URL =
            Options.key("api_base_url")
                    .stringType()
                    .defaultValue(DEFAULT_API_BASE_URL)
                    .withDescription("Mem0 Platform V3 API base URL");
    public static final Option<String> API_KEY =
            Options.key("api_key").stringType().noDefaultValue().withDescription("Mem0 API key");
    public static final Option<String> MESSAGES_FIELD =
            Options.key("messages_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Input row field containing the messages payload");
    public static final Option<String> USER_ID_FIELD =
            Options.key("user_id_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Input row field mapped to Mem0 user_id");
    public static final Option<String> AGENT_ID_FIELD =
            Options.key("agent_id_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional input row field mapped to Mem0 agent_id");
    public static final Option<String> APP_ID_FIELD =
            Options.key("app_id_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional input row field mapped to Mem0 app_id");
    public static final Option<String> RUN_ID_FIELD =
            Options.key("run_id_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional input row field mapped to Mem0 run_id");
    public static final Option<String> METADATA_FIELD =
            Options.key("metadata_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional input row field mapped to Mem0 metadata");
}
