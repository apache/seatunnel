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

