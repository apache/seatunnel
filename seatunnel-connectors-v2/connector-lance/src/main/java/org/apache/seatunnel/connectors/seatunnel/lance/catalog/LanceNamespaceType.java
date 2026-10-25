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
package org.apache.seatunnel.connectors.seatunnel.lance.catalog;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;

@VisibleForTesting
public enum LanceNamespaceType {
    REST("rest", "com.lancedb.lance.namespace.rest.RestNamespace"),
    DIRECTORY("dir", "com.lancedb.lance.namespace.dir.DirectoryNamespace"),
    HIVE2("hive2", "com.lancedb.lance.namespace.hive2.Hive2Namespace"),
    HIVE3("hive3", "com.lancedb.lance.namespace.hive3.Hive3Namespace"),
    GLUE("glue", "com.lancedb.lance.namespace.glue.GlueNamespace");

    final String type;
    final String impl;

    LanceNamespaceType(String type, String impl) {
        this.type = type;
        this.impl = impl;
    }

    public String getType() {
        return type;
    }

    public String getImpl() {
        return impl;
    }

    public static String ofImplByType(String type) {
        return Arrays.stream(LanceNamespaceType.values())
                .filter(vo -> vo.getType().equals(type))
                .findFirst()
                .map(LanceNamespaceType::getImpl)
                .orElse(null);
    }

    public static LanceNamespaceType typeOf(String type) {
        return Arrays.stream(LanceNamespaceType.values())
                .filter(vo -> vo.getType().equals(type))
                .findFirst()
                .orElse(null);
    }
}
