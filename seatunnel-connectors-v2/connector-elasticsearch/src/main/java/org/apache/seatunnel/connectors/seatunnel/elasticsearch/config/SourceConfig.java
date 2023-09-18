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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SourceConfig {

    public static final Option<String> INDEX =
            Options.key("index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Elasticsearch index name, support * fuzzy matching");

    public static final Option<List<String>> SOURCE =
            Options.key("source")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "The fields of index. You can get the document id by specifying the field _id.If sink _id to other index,you need specify an alias for _id due to the Elasticsearch limit");

    public static final Option<String> SCROLL_TIME =
            Options.key("scroll_time")
                    .stringType()
                    .defaultValue("1m")
                    .withDescription(
                            "Amount of time Elasticsearch will keep the search context alive for scroll requests");

    public static final Option<Integer> SCROLL_SIZE =
            Options.key("scroll_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Maximum number of hits to be returned with each Elasticsearch scroll request");

    public static final Option<Map> QUERY =
            Options.key("query")
                    .objectType(Map.class)
                    .defaultValue(
                            Collections.singletonMap("match_all", new HashMap<String, String>()))
                    .withDescription(
                            "Elasticsearch query language. You can control the range of data read");
}
