package org.apache.seatunnel.connectors.seatunnel.lance.catalog;

import com.google.common.annotations.VisibleForTesting;

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
}
