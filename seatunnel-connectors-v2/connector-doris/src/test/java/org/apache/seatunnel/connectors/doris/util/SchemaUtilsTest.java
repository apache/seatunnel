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

package org.apache.seatunnel.connectors.doris.util;

import org.apache.seatunnel.connectors.doris.rest.models.Schema;

import org.apache.doris.sdk.thrift.TPrimitiveType;
import org.apache.doris.sdk.thrift.TScanColumnDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class SchemaUtilsTest {

    @Test
    public void testConvertToSchemaWithUnknownType() {
        TScanColumnDesc known = new TScanColumnDesc();
        known.setName("id");
        known.setType(TPrimitiveType.INT);
        // a type the shaded thrift-service does not know, such as IPV4, deserializes to null
        TScanColumnDesc unknown = new TScanColumnDesc();
        unknown.setName("v4");

        Schema schema = SchemaUtils.convertToSchema(Arrays.asList(known, unknown));

        Assertions.assertEquals(2, schema.size());
        Assertions.assertEquals("INT", schema.getProperties().get(0).getType());
        Assertions.assertEquals("UNKNOWN", schema.getProperties().get(1).getType());
    }
}
