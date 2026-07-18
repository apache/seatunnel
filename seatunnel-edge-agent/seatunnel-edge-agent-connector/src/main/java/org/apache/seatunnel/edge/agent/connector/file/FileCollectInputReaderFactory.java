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

package org.apache.seatunnel.edge.agent.connector.file;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.edge.agent.connector.EdgeInputReader;
import org.apache.seatunnel.edge.agent.connector.EdgeInputReaderFactory;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePositionStore;
import org.apache.seatunnel.edge.agent.connector.config.FileCollectConfig;
import org.apache.seatunnel.edge.agent.connector.config.FileCollectOptionRules;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class FileCollectInputReaderFactory implements EdgeInputReaderFactory {

    @Override
    public String factoryIdentifier() {
        return "file";
    }

    @Override
    public OptionRule optionRule() {
        return FileCollectOptionRules.rule();
    }

    @Override
    public EdgeInputReader create(
            ReadonlyConfig inputConfig, EdgeSourcePositionStore sourcePositionStore) {
        return new FileCollectReader(FileCollectConfig.from(inputConfig), sourcePositionStore);
    }
}
