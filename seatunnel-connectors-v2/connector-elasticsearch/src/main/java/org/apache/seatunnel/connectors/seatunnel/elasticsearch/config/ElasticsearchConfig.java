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

import org.apache.seatunnel.api.table.catalog.CatalogTable;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class ElasticsearchConfig implements Serializable {

    private String index;
    private List<String> source;
    private Map<String, Object> query;
    private String scrollTime;
    private int scrollSize;

    private CatalogTable catalogTable;

    public ElasticsearchConfig clone() {
        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig();
        elasticsearchConfig.setIndex(index);
        elasticsearchConfig.setSource(new ArrayList<>(source));
        elasticsearchConfig.setQuery(new HashMap<>(query));
        elasticsearchConfig.setScrollTime(scrollTime);
        elasticsearchConfig.setScrollSize(scrollSize);
        elasticsearchConfig.setCatalogTable(catalogTable);
        return elasticsearchConfig;
    }
}
