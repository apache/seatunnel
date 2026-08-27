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

package org.apache.seatunnel.connectors.seatunnel.easysearch;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.easysearch.catalog.EasysearchDataTypeConvertor;

import org.apache.commons.collections4.CollectionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EasysearchSourceTest {

    @Test
    public void testPrepareWithEmptySource() throws PrepareFailException {
        List<String> source = Lists.newArrayList();

        Map<String, String> esFieldType = new HashMap<>();
        esFieldType.put("field1", "String");

        SeaTunnelRowType rowTypeInfo = null;
        EasysearchDataTypeConvertor EasySearchDataTypeConvertor = new EasysearchDataTypeConvertor();
        if (CollectionUtils.isEmpty(source)) {
            List<String> keys = new ArrayList<>(esFieldType.keySet());
            SeaTunnelDataType[] fieldTypes = new SeaTunnelDataType[keys.size()];
            for (int i = 0; i < keys.size(); i++) {
                String esType = esFieldType.get(keys.get(i));
                SeaTunnelDataType seaTunnelDataType =
                        EasySearchDataTypeConvertor.toSeaTunnelType(keys.get(i), esType);
                fieldTypes[i] = seaTunnelDataType;
            }
            rowTypeInfo = new SeaTunnelRowType(keys.toArray(new String[0]), fieldTypes);
        }

        Assertions.assertNotNull(rowTypeInfo);
        Assertions.assertEquals(rowTypeInfo.getFieldType(0), BasicType.STRING_TYPE);
    }
}
