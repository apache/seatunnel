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

package org.apache.seatunnel.connectors.doris.source.split;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.connectors.doris.rest.PartitionDefinition;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@AllArgsConstructor
@Getter
@Setter
public class DorisSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 8626697814676246066L;
    private final PartitionDefinition partitionDefinition;

    private final String splitId;

    @Override
    public String splitId() {
        return splitId;
    }

    public PartitionDefinition getPartitionDefinition() {
        return partitionDefinition;
    }

    @Override
    public String toString() {
        return String.format(
                "DorisSourceSplit: %s.%s,be=%s,tablets=%s",
                partitionDefinition.getDatabase(),
                partitionDefinition.getTable(),
                partitionDefinition.getBeAddress(),
                partitionDefinition.getTabletIds());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisSourceSplit that = (DorisSourceSplit) o;

        return Objects.equals(partitionDefinition, that.partitionDefinition);
    }
}
