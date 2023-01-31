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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.api.configuration.ConfigAdapterSpi;

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
    private static final List<ConfigAdapterSpi> CONFIG_ADAPTERS = new ArrayList<>(0);

    static {
        ServiceLoader<ConfigAdapterSpi> serviceLoader = ServiceLoader.load(ConfigAdapterSpi.class);
        Iterator<ConfigAdapterSpi> it = serviceLoader.iterator();
        if (it.hasNext()) {
            try {
                CONFIG_ADAPTERS.add(it.next());
            } catch (Exception loadSpiErr) {
                log.warn(loadSpiErr.getMessage());
            }
        }
    }

    public static Optional<ConfigAdapterSpi> selectAdapter(@NonNull String filePath) {
        for (ConfigAdapterSpi configGatewaySpi : CONFIG_ADAPTERS) {
            String extension = FileUtils.getFileExtension(filePath);
            if (configGatewaySpi.checkFileExtension(extension)) {
                try {
                    return Optional.of(configGatewaySpi);
                } catch (Exception warn) {
                    log.warn("Loading config failed with spi {}, fallback to HOCON loader.", configGatewaySpi.getClass().getName());
                    break;
                }
            }
        }
        return Optional.empty();
    }

    public static Optional<ConfigAdapterSpi> selectAdapter(@NonNull Path filePath) {
        return selectAdapter(filePath.getFileName().toString());
    }
}
