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

package org.apache.seatunnel.transform.calcite.adapter;

import org.apache.seatunnel.shade.org.apache.calcite.DataContext;
import org.apache.seatunnel.shade.org.apache.calcite.linq4j.Enumerable;
import org.apache.seatunnel.shade.org.apache.calcite.linq4j.Linq4j;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataType;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.seatunnel.shade.org.apache.calcite.schema.ScannableTable;
import org.apache.seatunnel.shade.org.apache.calcite.schema.impl.AbstractTable;

import lombok.Setter;

import java.util.Collections;

/**
 * A Calcite {@link ScannableTable} backed by a single SeaTunnel row. Each call to {@link
 * #scan(DataContext)} returns an {@link Enumerable} containing only the current row. The row is
 * injected before each SQL execution via {@link #setCurrentRow(Object[])}.
 */
public class SeaTunnelScannableTable extends AbstractTable implements ScannableTable {

    private final RelDataType rowType;

    @Setter private Object[] currentRow;

    public SeaTunnelScannableTable(RelDataType rowType) {
        this.rowType = rowType;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return rowType;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        if (currentRow == null) {
            return Linq4j.emptyEnumerable();
        }
        return Linq4j.asEnumerable(Collections.singletonList(currentRow));
    }
}
