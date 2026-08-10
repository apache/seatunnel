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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link DamengCatalogFactory}. */
public class DamengCatalogFactoryTest {

    @Test
    public void testFactoryIdentifier() {
        DamengCatalogFactory factory = new DamengCatalogFactory();
        Assertions.assertEquals(DatabaseIdentifier.DAMENG, factory.factoryIdentifier());
    }

    @Test
    public void testOptionRule() {
        DamengCatalogFactory factory = new DamengCatalogFactory();
        OptionRule optionRule = factory.optionRule();
        Assertions.assertNotNull(optionRule);
    }
}
