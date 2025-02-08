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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MaxcomputeSourceTest {

    @Test
    public void prepare() {
        Config fields =
                ConfigFactory.empty()
                        .withValue("id", ConfigValueFactory.fromAnyRef("int"))
                        .withValue("name", ConfigValueFactory.fromAnyRef("string"))
                        .withValue("age", ConfigValueFactory.fromAnyRef("int"));

        Config schema = fields.atKey("fields").atKey("schema");

        MaxcomputeSource maxcomputeSource = new MaxcomputeSource(ReadonlyConfig.fromConfig(schema));

        SeaTunnelRowType seaTunnelRowType =
                maxcomputeSource.getProducedCatalogTables().get(0).getSeaTunnelRowType();
        Assertions.assertEquals(SqlType.INT, seaTunnelRowType.getFieldType(0).getSqlType());
    }
}
