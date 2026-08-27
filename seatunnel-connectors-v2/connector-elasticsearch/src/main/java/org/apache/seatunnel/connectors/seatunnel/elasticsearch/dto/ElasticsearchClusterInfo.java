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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto;

import org.apache.seatunnel.shade.com.google.common.base.Strings;

import org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.ElasticsearchVersion;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class ElasticsearchClusterInfo {
    private String distribution;
    private String clusterVersion;

    public ElasticsearchVersion getElasticsearchVersion() {
        return ElasticsearchVersion.get(clusterVersion);
    }

    public boolean isOpensearch() {
        return !Strings.isNullOrEmpty(distribution) && "opensearch".equalsIgnoreCase(distribution);
    }
}
