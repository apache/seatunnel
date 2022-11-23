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

package org.apache.seatunnel.connectors.seatunnel.sentry.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class SentryConfig {

    public static final String SENTRY = "Sentry";

    public static final Option<String> DSN = Options.key("dsn").stringType().noDefaultValue().withDescription("sentry dsn");
    public static final Option<String> ENV = Options.key("env").stringType().noDefaultValue().withDescription("env");
    public static final Option<String> RELEASE = Options.key("release").stringType().noDefaultValue().withDescription("release");
    public static final Option<String> CACHE_DIRPATH = Options.key("cacheDirPath").stringType().noDefaultValue().withDescription("sentry cache dir path");
    public static final Option<String> ENABLE_EXTERNAL_CONFIGURATION = Options.key("enableExternalConfiguration").stringType().noDefaultValue().withDescription("enable external configuration");
    public static final Option<String> MAX_CACHEITEMS = Options.key("maxCacheItems").stringType().noDefaultValue().withDescription("max cache items");
    public static final Option<String> FLUSH_TIMEOUTMILLIS = Options.key("flushTimeoutMillis").stringType().noDefaultValue().withDescription("flush timeout millis");
    public static final Option<String> MAX_QUEUESIZE = Options.key("maxQueueSize").stringType().noDefaultValue().withDescription("flush queue size");
}
