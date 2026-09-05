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

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.MySqlIncrementalSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests the OceanBase CDC wrapper contract that differentiates it from the reused MySQL CDC
 * implementation.
 */
public class OceanBaseIncrementalSourceFactoryTest {

    /** Verify the OceanBase wrapper uses a dedicated factory identifier for plugin discovery. */
    @Test
    public void testFactoryIdentifier() {
        Assertions.assertEquals(
                "OceanBase-CDC", new OceanBaseIncrementalSourceFactory().factoryIdentifier());
    }

    /** Verify the wrapper keeps the MySQL factory inheritance so restore logic stays aligned. */
    @Test
    public void testFactoryInheritance() {
        Assertions.assertEquals(
                MySqlIncrementalSourceFactory.class,
                OceanBaseIncrementalSourceFactory.class.getSuperclass());
    }

    /** Verify the factory returns the OceanBase wrapper source instead of the raw MySQL source. */
    @Test
    public void testSourceClass() {
        Assertions.assertEquals(
                OceanBaseIncrementalSource.class,
                new OceanBaseIncrementalSourceFactory().getSourceClass());
    }
}
