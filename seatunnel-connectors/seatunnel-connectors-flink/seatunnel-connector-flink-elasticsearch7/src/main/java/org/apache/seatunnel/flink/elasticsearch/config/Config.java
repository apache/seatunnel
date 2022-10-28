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

package org.apache.seatunnel.flink.elasticsearch.config;

/**
 * ElasticSearch sink configuration options
 */
public final class Config {

    private Config() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Parallelism of sink
     */
    public static final String PARALLELISM = "parallelism";

    /**
     * ElasticSearch index
     */
    public static final String INDEX = "index";

    /**
     * ElasticSearch index type
     */
    public static final String INDEX_TYPE = "index_type";

    /**
     * ElasticSearch index time format
     */
    public static final String INDEX_TIME_FORMAT = "index_time_format";

    /**
     * ElasticSearch hosts (separated by commma)
     */
    public static final String HOSTS = "hosts";

    /**
     * Default index name
     */
    public static final String DEFAULT_INDEX = "seatunnel";

    /**
     * Default index time format
     */
    public static final String DEFAULT_INDEX_TIME_FORMAT = "yyyy.MM.dd";

}
