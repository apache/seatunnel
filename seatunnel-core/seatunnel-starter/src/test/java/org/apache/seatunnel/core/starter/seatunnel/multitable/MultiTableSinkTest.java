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

package org.apache.seatunnel.core.starter.seatunnel.multitable;

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.e2e.sink.inmemory.InMemoryAggregatedCommitter;
import org.apache.seatunnel.e2e.sink.inmemory.InMemorySinkWriter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Order(1)
public class MultiTableSinkTest {

    @Test
    @DisabledOnOs(value = {OS.WINDOWS})
    public void testMultiTableSink()
            throws FileNotFoundException, URISyntaxException, CommandException {
        String configurePath = "/config/fake_to_inmemory_multi_table.conf";
        String configFile = getTestConfigFile(configurePath);
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setConfigFile(configFile);
        clientCommandArgs.setCheckConfig(false);
        clientCommandArgs.setJobName(Paths.get(configFile).getFileName().toString());
        clientCommandArgs.setMasterType(MasterType.LOCAL);
        SeaTunnel.run(clientCommandArgs.buildCommand());
        List<String> writerEvents = InMemorySinkWriter.getEvents();
        Assertions.assertEquals(1, InMemorySinkWriter.getResourceManagers().size());
        List<String> resourceManagersEvents =
                InMemorySinkWriter.getResourceManagers().get(0).getEvent();
        List<String> aggregatedEvents = InMemoryAggregatedCommitter.getEvents();
        Assertions.assertEquals(1, InMemoryAggregatedCommitter.getResourceManagers().size());
        List<String> committerResourceManagersEvents =
                InMemoryAggregatedCommitter.getResourceManagers().get(0).getEvent();

        Assertions.assertIterableEquals(
                Arrays.asList("initMultiTableResourceManager1", "setMultiTableResourceManager0"),
                writerEvents);
        Assertions.assertIterableEquals(
                Collections.singletonList("InMemoryMultiTableResourceManager::close"),
                resourceManagersEvents);
        Assertions.assertIterableEquals(
                Arrays.asList("initMultiTableResourceManager1", "setMultiTableResourceManager0"),
                aggregatedEvents);
        Assertions.assertIterableEquals(
                Collections.singletonList("InMemoryMultiTableResourceManager::close"),
                committerResourceManagersEvents);
    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = MultiTableSinkTest.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
