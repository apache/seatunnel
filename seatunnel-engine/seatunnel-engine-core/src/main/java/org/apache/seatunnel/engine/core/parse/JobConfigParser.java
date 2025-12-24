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

package org.apache.seatunnel.engine.core.parse;

import org.apache.seatunnel.engine.common.utils.IdGenerator;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.Data;
import lombok.NonNull;

import java.net.URL;
import java.util.List;

@Data
public class JobConfigParser {
    private static final ILogger LOGGER = Logger.getLogger(JobConfigParser.class);
    private IdGenerator idGenerator;
    private boolean isStartWithSavePoint;
    private MultipleTableJobConfigParser multipleTableJobConfigParser;
    private List<URL> commonPluginJars;

    public JobConfigParser(
            @NonNull IdGenerator idGenerator,
            @NonNull List<URL> commonPluginJars,
            MultipleTableJobConfigParser multipleTableJobConfigParser,
            boolean isStartWithSavePoint) {
        this.idGenerator = idGenerator;
        this.commonPluginJars = commonPluginJars;
        this.multipleTableJobConfigParser = multipleTableJobConfigParser;
        this.isStartWithSavePoint = isStartWithSavePoint;
    }

    static String createSourceActionName(int configIndex, String pluginName) {
        return String.format("Source[%s]-%s", configIndex, pluginName);
    }

    static String createSinkActionName(int configIndex, String pluginName, String table) {
        return String.format("Sink[%s]-%s-%s", configIndex, pluginName, table);
    }

    static String createTransformActionName(int configIndex, String pluginName) {
        return String.format("Transform[%s]-%s", configIndex, pluginName);
    }
}
