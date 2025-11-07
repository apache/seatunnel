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

package org.apache.seatunnel.e2e.common.util;

import org.apache.seatunnel.api.configuration.ConfigAdapter;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

@Slf4j
public final class ConfigAdapterUtils {
    private static final List<ConfigAdapter> CONFIG_ADAPTERS = new ArrayList<>(0);

    static {
        ServiceLoader<ConfigAdapter> serviceLoader = ServiceLoader.load(ConfigAdapter.class);
        Iterator<ConfigAdapter> it = serviceLoader.iterator();
        it.forEachRemaining(CONFIG_ADAPTERS::add);
    }

    public static Optional<ConfigAdapter> selectAdapter(@NonNull String filePath) {
        for (ConfigAdapter configAdapter : CONFIG_ADAPTERS) {
            int extIdx = filePath.lastIndexOf(".");
            String extension = filePath.substring(extIdx + 1);
            for (String extensionIdentifier :
                    ArrayUtils.nullToEmpty(configAdapter.extensionIdentifiers())) {
                if (StringUtils.equalsIgnoreCase(extension, extensionIdentifier)) {
                    return Optional.of(configAdapter);
                }
            }
        }
        return Optional.empty();
    }

    public static Optional<ConfigAdapter> selectAdapter(@NonNull Path filePath) {
        return selectAdapter(filePath.getFileName().toString());
    }
}
