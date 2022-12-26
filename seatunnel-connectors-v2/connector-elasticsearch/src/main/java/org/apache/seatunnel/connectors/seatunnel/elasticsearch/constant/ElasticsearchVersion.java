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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant;

import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;

public enum ElasticsearchVersion {
    ES2(2), ES5(5), ES6(6), ES7(7), ES8(8);

    private int version;

    ElasticsearchVersion(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public static ElasticsearchVersion get(int version) {
        for (ElasticsearchVersion elasticsearchVersion : ElasticsearchVersion.values()) {
            if (elasticsearchVersion.getVersion() == version) {
                return elasticsearchVersion;
            }
        }
        throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.GET_ES_VERSION_FAILED,
            String.format("version=%d,fail fo find ElasticsearchVersion.", version));
    }

    public static ElasticsearchVersion get(String clusterVersion) {
        String[] versionArr = clusterVersion.split("\\.");
        int version = Integer.parseInt(versionArr[0]);
        return get(version);
    }
}
