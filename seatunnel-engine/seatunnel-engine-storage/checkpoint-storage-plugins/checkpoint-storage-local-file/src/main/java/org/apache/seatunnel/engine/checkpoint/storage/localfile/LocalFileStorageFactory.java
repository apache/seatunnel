/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.localfile;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;

import com.google.auto.service.AutoService;

import java.util.Map;

/**
 * Local file storage plug-in, use local file storage,
 * only suitable for single-machine testing or small data scale use, use with caution in production environment
 * <p>
 * deprecated: use @see org.apache.seatunnel.engine.checkpoint.storage.hdfs.HdfsStorageFactory instead
 */
@Deprecated
@AutoService(Factory.class)
public class LocalFileStorageFactory implements CheckpointStorageFactory {

    @Override
    public String factoryIdentifier() {
        return "localfile";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }

    @Override
    public CheckpointStorage create(Map<String, String> configuration) {
        return new LocalFileStorage(configuration);
    }
}
