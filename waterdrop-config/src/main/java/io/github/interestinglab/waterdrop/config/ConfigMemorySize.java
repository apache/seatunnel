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
/**
 *   Copyright (C) 2015 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config;

/**
 * An immutable class representing an amount of memory.  Use
 * static factory methods such as {@link
 * ConfigMemorySize#ofBytes(long)} to create instances.
 *
 * @since 1.3.0
 */
public final class ConfigMemorySize {
    private final long bytes;

    private ConfigMemorySize(long bytes) {
        if (bytes < 0)
            throw new IllegalArgumentException("Attempt to construct ConfigMemorySize with negative number: " + bytes);
        this.bytes = bytes;
    }

    /**
     * Constructs a ConfigMemorySize representing the given
     * number of bytes.
     * @since 1.3.0
     * @param bytes a number of bytes
     * @return an instance representing the number of bytes
     */
    public static ConfigMemorySize ofBytes(long bytes) {
        return new ConfigMemorySize(bytes);
    }

    /**
     * Gets the size in bytes.
     * @since 1.3.0
     * @return how many bytes
     */
    public long toBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return "ConfigMemorySize(" + bytes + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ConfigMemorySize) {
            return ((ConfigMemorySize)other).bytes == this.bytes;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        // in Java 8 this can become Long.hashCode(bytes)
        return Long.valueOf(bytes).hashCode();
    }

}

