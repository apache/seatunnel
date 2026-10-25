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

package org.apache.seatunnel.api.metalake;

import org.apache.seatunnel.api.metalake.gravitino.GravitinoClient;
import org.apache.seatunnel.api.metalake.gravitino.GravitinoTableSchemaConvertor;
import org.apache.seatunnel.common.constants.MetaLakeType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class MetaLakeFactory {

    private static final Map<String, Supplier<MetalakeClient>> CLIENT_REGISTRY = new HashMap<>();
    private static final Map<String, Supplier<MetaLakeTableSchemaConvertor>> MAPPER_REGISTRY =
            new HashMap<>();

    static {
        register(MetaLakeType.GRAVITINO.getType());
    }

    private MetaLakeFactory() {}

    public static void register(String type) {
        CLIENT_REGISTRY.put(type.toLowerCase(), GravitinoClient::new);
        MAPPER_REGISTRY.put(type.toLowerCase(), GravitinoTableSchemaConvertor::new);
    }

    public static MetalakeClient createClient(MetaLakeType metaLakeType) {
        String type = metaLakeType.name().toLowerCase();
        Supplier<MetalakeClient> constructor = CLIENT_REGISTRY.get(type.toLowerCase());
        if (constructor == null) {
            throw new IllegalArgumentException("Unknown MetalakeClient type: " + type);
        }
        return constructor.get();
    }

    public static MetaLakeTableSchemaConvertor createTypeMapper(MetaLakeType metaLakeType) {
        String type = metaLakeType.name().toLowerCase();
        Supplier<MetaLakeTableSchemaConvertor> constructor =
                MAPPER_REGISTRY.get(type.toLowerCase());
        if (constructor == null) {
            throw new IllegalArgumentException("Unknown MetaLakeTypeMapper type: " + type);
        }
        return constructor.get();
    }
}
