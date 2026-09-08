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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.source;

import org.apache.seatunnel.api.source.SourceSplit;

/**
 * Descriptor for the V1 collection scan.
 *
 * <p>Only the split id, filter, and projection are durable. There is no continuation token or
 * cursor position, so restoring this split reruns its query from the beginning.
 */
public class AmazonDocumentDBSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final Integer splitId;
    private final String matchQuery;
    private final String projection;

    public AmazonDocumentDBSourceSplit(Integer splitId, String matchQuery, String projection) {
        this.splitId = splitId;
        this.matchQuery = matchQuery;
        this.projection = projection;
    }

    public Integer getSplitId() {
        return splitId;
    }

    public String getMatchQuery() {
        return matchQuery;
    }

    public String getProjection() {
        return projection;
    }

    @Override
    public String splitId() {
        return splitId.toString();
    }

    public AmazonDocumentDBSourceSplit copy() {
        return new AmazonDocumentDBSourceSplit(splitId, matchQuery, projection);
    }
}
