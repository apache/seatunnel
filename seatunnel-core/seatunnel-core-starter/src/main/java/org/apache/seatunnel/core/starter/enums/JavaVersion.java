/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.core.starter.enums;

import org.apache.commons.lang3.math.NumberUtils;

/**
 * An enum representing all the versions of the Java specification. This is intended to mirror
 * available values from the <em>java.specification.version</em> System property.
 *
 * @since 3.0
 */
public enum JavaVersion {

    /** The Java version reported by Android. This is not an official Java version number. */
    JAVA_0_9(1.5f, "0.9"),

    /** Java 1.1. */
    JAVA_1_1(1.1f, "1.1"),

    /** Java 1.2. */
    JAVA_1_2(1.2f, "1.2"),

    /** Java 1.3. */
    JAVA_1_3(1.3f, "1.3"),

    /** Java 1.4. */
    JAVA_1_4(1.4f, "1.4"),

    /** Java 1.5. */
    JAVA_1_5(1.5f, "1.5"),

    /** Java 1.6. */
    JAVA_1_6(1.6f, "1.6"),

    /** Java 1.7. */
    JAVA_1_7(1.7f, "1.7"),

    /** Java 1.8. */
    JAVA_1_8(1.8f, "1.8"),

    /**
     * Java 1.9.
     *
     * @deprecated As of release 3.5, replaced by {@link #JAVA_9}
     */
    @Deprecated
    JAVA_1_9(9.0f, "9"),

    /**
     * Java 9
     *
     * @since 3.5
     */
    JAVA_9(9.0f, "9"),

    /**
     * Java 10
     *
     * @since 3.7
     */
    JAVA_10(10.0f, "10"),

    /**
     * Java 11
     *
     * @since 3.8
     */
    JAVA_11(11.0f, "11"),

    /**
     * The most recent java version. Mainly introduced to avoid to break when a new version of Java
     * is used.
     */
    JAVA_RECENT(maxVersion(), Float.toString(maxVersion()));

    /** The float value. */
    private final float value;

    /** The standard name. */
    private final String name;

    /**
     * Constructor.
     *
     * @param value the float value
     * @param name the standard name, not null
     */
    JavaVersion(final float value, final String name) {
        this.value = value;
        this.name = name;
    }

    // -----------------------------------------------------------------------
    /**
     * Whether this version of Java is at least the version of Java passed in.
     *
     * <p>For example:<br>
     * {@code myVersion.atLeast(JavaVersion.JAVA_1_4)}
     *
     * <p>
     *
     * @param requiredVersion the version to check against, not null
     * @return true if this version is equal to or greater than the specified version
     */
    public boolean atLeast(final JavaVersion requiredVersion) {
        return this.value >= requiredVersion.value;
    }

    /**
     * Transforms the given string with a Java version number to the corresponding constant of this
     * enumeration class. This method is used internally.
     *
     * @param nom the Java version as string
     * @return the corresponding enumeration constant or <b>null</b> if the version is unknown
     */
    // helper for static importing
    static JavaVersion getJavaVersion(final String nom) {
        return get(nom);
    }

    /**
     * Transforms the given string with a Java version number to the corresponding constant of this
     * enumeration class. This method is used internally.
     *
     * @param nom the Java version as string
     * @return the corresponding enumeration constant or <b>null</b> if the version is unknown
     */
    public static JavaVersion get(final String nom) {
        if ("0.9".equals(nom)) {
            return JAVA_0_9;
        } else if ("1.1".equals(nom)) {
            return JAVA_1_1;
        } else if ("1.2".equals(nom)) {
            return JAVA_1_2;
        } else if ("1.3".equals(nom)) {
            return JAVA_1_3;
        } else if ("1.4".equals(nom)) {
            return JAVA_1_4;
        } else if ("1.5".equals(nom)) {
            return JAVA_1_5;
        } else if ("1.6".equals(nom)) {
            return JAVA_1_6;
        } else if ("1.7".equals(nom)) {
            return JAVA_1_7;
        } else if ("1.8".equals(nom)) {
            return JAVA_1_8;
        } else if ("9".equals(nom)) {
            return JAVA_9;
        } else if ("10".equals(nom)) {
            return JAVA_10;
        } else if ("11".equals(nom)) {
            return JAVA_11;
        }
        if (nom == null) {
            return null;
        }
        final float v = toFloatVersion(nom);
        if ((v - 1.) < 1.) { // then we need to check decimals > .9
            final int firstComma = Math.max(nom.indexOf('.'), nom.indexOf(','));
            final int end = Math.max(nom.length(), nom.indexOf(',', firstComma));
            if (Float.parseFloat(nom.substring(firstComma + 1, end)) > .9f) {
                return JAVA_RECENT;
            }
        } else if (v > 10) {
            return JAVA_RECENT;
        }
        return null;
    }

    // -----------------------------------------------------------------------
    /**
     * The string value is overridden to return the standard name.
     *
     * <p>For example, <code>"1.5"</code>.
     *
     * @return the name, not null
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * Gets the Java Version from the system or 99.0 if the {@code java.specification.version}
     * system property is not set.
     *
     * @return the value of {@code java.specification.version} system property or 99.0 if it is not
     *     set.
     */
    private static float maxVersion() {
        final float v = toFloatVersion(System.getProperty("java.specification.version", "99.0"));
        if (v > 0) {
            return v;
        }
        return 99f;
    }

    /**
     * Parses a float value from a String.
     *
     * @param value the String to parse.
     * @return the float value represented by the string or -1 if the given String can not be
     *     parsed.
     */
    private static float toFloatVersion(final String value) {
        final int defaultReturnValue = -1;
        if (value.contains(".")) {
            final String[] toParse = value.split("\\.");
            if (toParse.length >= 2) {
                return NumberUtils.toFloat(toParse[0] + '.' + toParse[1], defaultReturnValue);
            }
        } else {
            return NumberUtils.toFloat(value, defaultReturnValue);
        }
        return defaultReturnValue;
    }
}
