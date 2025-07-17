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

package org.apache.seatunnel.api.table.catalog;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Represent a physical table schema. */
@EqualsAndHashCode(callSuper = true)
@Data
public final class MetadataSchema extends AbstractSchema {
    private static final long serialVersionUID = 1L;

    public MetadataSchema(List<Column> columns) {
        super(columns);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final List<Column> columns = new ArrayList<>();

        public Builder columns(List<Column> columns) {
            this.columns.addAll(columns);
            return this;
        }

        public Builder column(Column column) {
            this.columns.add(column);
            return this;
        }

        public MetadataSchema build() {
            return new MetadataSchema(columns);
        }
    }

    public MetadataSchema copy() {
        List<Column> copyColumns = columns.stream().map(Column::copy).collect(Collectors.toList());
        return MetadataSchema.builder().columns(copyColumns).build();
    }
}
