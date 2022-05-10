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

package org.apache.seatunnel.translation.spark.serialization;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.serialization.RowSerialization;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.IOException;

public class SparkRowSerialization implements RowSerialization<Row> {

    @Override
    public Row serialize(SeaTunnelRow seaTunnelRow) throws IOException {
        return RowFactory.create(seaTunnelRow.getFields());
    }

    @Override
    public SeaTunnelRow deserialize(Row engineRow) throws IOException {
        Object[] fields = new Object[engineRow.length()];
        for (int i = 0; i < engineRow.length(); i++) {
            fields[i] = engineRow.get(i);
        }
        return new SeaTunnelRow(fields);
    }
}
