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

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.source;

import org.apache.seatunnel.api.source.SourceSplit;

public class AzureCosmosDBSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 2485413678354889739L;

    private final Integer splitId;
    private String continuationToken;

    public AzureCosmosDBSourceSplit(Integer splitId) {
        this(splitId, null);
    }

    public AzureCosmosDBSourceSplit(Integer splitId, String continuationToken) {
        this.splitId = splitId;
        this.continuationToken = continuationToken;
    }

    public Integer getSplitId() {
        return splitId;
    }

    public String getContinuationToken() {
        return continuationToken;
    }

    public void setContinuationToken(String continuationToken) {
        this.continuationToken = continuationToken;
    }

    @Override
    public String splitId() {
        return splitId.toString();
    }

    public AzureCosmosDBSourceSplit copy() {
        return new AzureCosmosDBSourceSplit(splitId, continuationToken);
    }
}
