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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbOption.COLLECTION;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbOption.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbOption.MATCHQUERY;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbOption.URI;

/** The config of mongodb */
@Builder
@Getter
public class MongodbConfig implements Serializable {

    @Builder.Default private String uri = URI.defaultValue();
    @Builder.Default private String database = DATABASE.defaultValue();
    @Builder.Default private String collection = COLLECTION.defaultValue();
    @Builder.Default private String matchQuery = MATCHQUERY.defaultValue();

    public static MongodbConfig buildWithConfig(Config config) {
        MongodbConfigBuilder builder = MongodbConfig.builder();
        if (config.hasPath(URI.key())) {
            builder.uri(config.getString(URI.key()));
        }
        if (config.hasPath(DATABASE.key())) {
            builder.database(config.getString(DATABASE.key()));
        }
        if (config.hasPath(COLLECTION.key())) {
            builder.collection(config.getString(COLLECTION.key()));
        }
        if (config.hasPath(MATCHQUERY.key())) {
            builder.matchQuery(config.getString(MATCHQUERY.key()));
        }
        return builder.build();
    }
}
