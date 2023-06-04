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

package org.apache.seatunnel.connectors.seatunnel.milvus.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.SeaTunnelSink;

import java.io.Serializable;

/**
 * Utility class to milvus configuration options, used by {@link SeaTunnelSink}.
 */
public class MilvusConfig implements Serializable {


    public static final Option<String> MILVUS_HOST =
            Options.key("milvus_host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The milvus host");

    public static final Option<Integer> MILVUS_PORT =
            Options.key("milvus_port")
                    .intType()
                    .defaultValue(19530)
                    .withDescription("This port is for gRPC. Default is 19530");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username of milvus server.");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password of milvus server.");

    public static final Option<String> COLLECTION_NAME =
            Options.key("collection_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("A collection of milvus, which is similar to a table in a relational database.");

    public static final Option<String> PARTITION_FIELD =
            Options.key("partition_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Partition fields, which must be included in the collection's schema.");


    public static final Option<String> OPENAI_ENGINE =
            Options.key("openai_engine")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Text embedding model. Default is 'text-embedding-ada-002'");

    public static final Option<String> OPENAI_API_KEY =
            Options.key("openai_api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Use your own Open AI API Key here.");



    public static final Option<Integer> DIMENSION =
            Options.key("dimension")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Embeddings size.");



    public static final Option<String> EMBEDDINGS_FIELDS =
            Options.key("embeddings_fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Fields to be embedded,They use`,`for splitting");



}
