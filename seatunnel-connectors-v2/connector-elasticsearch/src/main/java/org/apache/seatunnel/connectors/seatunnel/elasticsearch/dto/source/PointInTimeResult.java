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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/** DTO for Elasticsearch Point-in-Time search results. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PointInTimeResult {

    /** The PIT ID used for this search */
    private String pitId;

    /** Documents returned by the search */
    private List<Map<String, Object>> docs;

    /** Total number of hits matching the query */
    private long totalHits;

    /** Sort values of the last document, used for pagination with search_after */
    private Object[] searchAfter;

    /** Whether there are more results to fetch */
    private boolean hasMore;
}
