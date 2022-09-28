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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.type;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.ElasticsearchVersion.ES2;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.ElasticsearchVersion.ES5;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.ElasticsearchVersion.ES6;

import org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.ElasticsearchVersion;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.type.impl.NotIndexTypeSerializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.type.impl.RequiredIndexTypeSerializer;

public class IndexTypeSerializerFactory {

    private static final String DEFAULT_TYPE = "st";

    private IndexTypeSerializerFactory() {

    }

    public static IndexTypeSerializer getIndexTypeSerializer(ElasticsearchVersion elasticsearchVersion, String type) {
        if (elasticsearchVersion == ES2 || elasticsearchVersion == ES5) {
            if (type == null || "".equals(type)) {
                type = DEFAULT_TYPE;
            }
            return new RequiredIndexTypeSerializer(type);
        }
        if (elasticsearchVersion == ES6) {
            if (type != null && !"".equals(type)) {
                return new RequiredIndexTypeSerializer(type);
            }
        }
        return new NotIndexTypeSerializer();
    }

}
