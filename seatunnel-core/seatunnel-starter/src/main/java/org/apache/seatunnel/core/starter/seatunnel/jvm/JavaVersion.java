/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.seatunnel.core.starter.seatunnel.jvm;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JavaVersion {

    public static final List<Integer> CURRENT = parse(System.getProperty("java.specification.version"));

    static List<Integer> parse(final String value) {
        if (!value.matches("^0*[0-9]+(\\.[0-9]+)*$")) {
            throw new IllegalArgumentException(value);
        }

        final List<Integer> version = new ArrayList<Integer>();
        final String[] components = value.split("\\.");
        for (final String component : components) {
            version.add(Integer.valueOf(component));
        }
        return version;
    }

    public static int majorVersion(final List<Integer> javaVersion) {
        Objects.requireNonNull(javaVersion);
        if (javaVersion.get(0) > 1) {
            return javaVersion.get(0);
        } else {
            return javaVersion.get(1);
        }
    }
}
