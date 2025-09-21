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

package org.apache.seatunnel.connectors.seatunnel.lance.utils;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** The converter for converting {@link Fragment} and {@link SeaTunnelRow} * */
public class FragmentConverter {

    private FragmentConverter() {}

    public static List<FragmentMetadata> reconvert(
            SeaTunnelRow seaTunnelRow, SeaTunnelRowType seaTunnelRowType, String datasetUri) {

        return new ArrayList<>();
    }

    public static Schema convertSchema(SeaTunnelRow row) {
        Object[] fields = row.getFields();
        if (Objects.nonNull(fields)) {
            for (Object f : fields) {}
        }
        return new Schema(Arrays.asList(Field.nullable("id", new ArrowType.Int(32, true))));
    }
}
