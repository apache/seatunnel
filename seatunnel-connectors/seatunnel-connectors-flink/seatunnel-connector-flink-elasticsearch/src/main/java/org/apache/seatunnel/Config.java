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

package org.apache.seatunnel;

/**
 * ElasticSearch sink configuration options
 */
public interface Config {

    /**
     * Parallelism of sink
     */
    String PARALLELISM = "parallelism";

    /**
     * ElasticSearch index
     */
    String INDEX = "index";

    /**
     * ElasticSearch index type
     */
    String INDEX_TYPE = "index_type";

    /**
     * ElasticSearch index time format
     */
    String INDEX_TIME_FORMAT = "index_time_format";

    /**
     * ElasticSearch hosts (separated by commma)
     */
    String HOSTS = "hosts";

    /**
     * Default index type
     */
    String DEFAULT_INDEX_TYPE = "log";

    /**
     * Default index name
     */
    String DEFAULT_INDEX = "seatunnel";

    /**
     * Default index time format
     */
    String DEFAULT_INDEX_TIME_FORMAT = "yyyy.MM.dd";

}
