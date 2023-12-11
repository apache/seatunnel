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

package org.apache.seatunnel.api.configuration;

import java.nio.file.Path;
import java.util.Map;

/** Adapter mode to support convert other config to HOCON. */
public interface ConfigAdapter {

    /**
     * Provides the config file extension identifier supported by the adapter.
     *
     * @return Extension identifier.
     */
    String[] extensionIdentifiers();

    /**
     * Converter config file to path_key-value Map in HOCON
     *
     * @param configFilePath config file path.
     * @return Map
     */
    Map<String, Object> loadConfig(Path configFilePath);
}
