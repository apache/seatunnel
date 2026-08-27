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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.source.ElasticsearchSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchSourceTest {
    @Test
    public void testPrepareWithEmptySource() throws PrepareFailException {
        BasicTypeDefine.BasicTypeDefineBuilder<EsType> typeDefine =
                BasicTypeDefine.<EsType>builder()
                        .name("field1")
                        .columnType("text")
                        .dataType("text");
        Map<String, BasicTypeDefine<EsType>> esFieldType = new HashMap<>();
        esFieldType.put("field1", typeDefine.build());
        SeaTunnelDataType[] seaTunnelDataTypes =
                ElasticsearchSource.getSeaTunnelDataType(
                        esFieldType, new ArrayList<>(esFieldType.keySet()));
        Assertions.assertNotNull(seaTunnelDataTypes);
        Assertions.assertEquals(seaTunnelDataTypes[0].getTypeClass(), String.class);
    }
}
