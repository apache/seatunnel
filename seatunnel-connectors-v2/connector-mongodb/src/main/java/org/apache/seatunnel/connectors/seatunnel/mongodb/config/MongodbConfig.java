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

package org.apache.seatunnel.connectors.seatunnel.mongodb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;

/**
 * The config of mongodb
 */
public class MongodbConfig implements Serializable {

    public static final Option<String> URI =
        Options.key("uri")
            .stringType()
            .noDefaultValue()
            .withDescription("MongoDB uri");

    public static final Option<String> DATABASE =
        Options.key("database")
            .stringType()
            .noDefaultValue()
            .withDescription("MongoDB database name");

    public static final Option<String> COLLECTION =
        Options.key("collection")
            .stringType()
            .noDefaultValue()
            .withDescription("MongoDB collection");

    // Don't use now
    public static final String FORMAT = "format";

    // Don't use now
    public static final String DEFAULT_FORMAT = "json";

}
