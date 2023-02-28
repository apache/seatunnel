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

package org.apache.seatunnel.connectors.doris.rest.models;

import java.util.Map;
import java.util.Objects;

public class QueryPlan {
    private int status;
    private String opaquedQueryPlan;
    private Map<String, Tablet> partitions;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getOpaquedQueryPlan() {
        return opaquedQueryPlan;
    }

    public void setOpaquedQueryPlan(String opaquedQueryPlan) {
        this.opaquedQueryPlan = opaquedQueryPlan;
    }

    public Map<String, Tablet> getPartitions() {
        return partitions;
    }

    public void setPartitions(Map<String, Tablet> partitions) {
        this.partitions = partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryPlan queryPlan = (QueryPlan) o;
        return status == queryPlan.status
                && Objects.equals(opaquedQueryPlan, queryPlan.opaquedQueryPlan)
                && Objects.equals(partitions, queryPlan.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, opaquedQueryPlan, partitions);
    }
}
