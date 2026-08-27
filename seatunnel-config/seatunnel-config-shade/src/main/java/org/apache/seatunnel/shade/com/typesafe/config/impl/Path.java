/*
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */

package org.apache.seatunnel.shade.com.typesafe.config.impl;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;

import java.util.Iterator;
import java.util.List;

final class Path {

    private final String first;
    private final Path remainder;
    private static final int DEFAULT_VALUE = 41;

    Path(String first, Path remainder) {
        this.first = first;
        this.remainder = remainder;
    }

    Path(String... elements) {
        if (elements.length == 0) {
            throw new ConfigException.BugOrBroken("empty path");
        }
        this.first = elements[0];
        if (elements.length > 1) {
            PathBuilder pb = new PathBuilder();
            for (int i = 1; i < elements.length; ++i) {
                pb.appendKey(elements[i]);
            }
            this.remainder = pb.result();
        } else {
            this.remainder = null;
        }
    }

    // append all the paths in the list together into one path
    Path(List<Path> pathsToConcat) {
        this(pathsToConcat.iterator());
    }

    // append all the paths in the iterator together into one path
    Path(Iterator<Path> i) {
        if (!i.hasNext()) {
            throw new ConfigException.BugOrBroken("empty path");
        }

        Path firstPath = i.next();
        this.first = firstPath.first;

        PathBuilder pb = new PathBuilder();
        if (firstPath.remainder != null) {
            pb.appendPath(firstPath.remainder);
        }
        while (i.hasNext()) {
            pb.appendPath(i.next());
        }
        this.remainder = pb.result();
    }

    String first() {
        return first;
    }

    /** @return path minus the first element or null if no more elements */
    Path remainder() {
        return remainder;
    }

    /** @return path minus the last element or null if we have just one element */
    Path parent() {
        if (remainder == null) {
            return null;
        }

        PathBuilder pb = new PathBuilder();
        Path p = this;
        while (p.remainder != null) {
            pb.appendKey(p.first);
            p = p.remainder;
        }
        return pb.result();
    }

    /** @return last element in the path */
    String last() {
        Path p = this;
        while (p.remainder != null) {
            p = p.remainder;
        }
        return p.first;
    }

    Path prepend(Path toPrepend) {
        PathBuilder pb = new PathBuilder();
        pb.appendPath(toPrepend);
        pb.appendPath(this);
        return pb.result();
    }

    int length() {
        int count = 1;
        Path p = remainder;
        while (p != null) {
            count += 1;
            p = p.remainder;
        }
        return count;
    }

    Path subPath(int removeFromFront) {
        int count = removeFromFront;
        Path p = this;
        while (p != null && count > 0) {
            count -= 1;
            p = p.remainder;
        }
        return p;
    }

    Path subPath(int firstIndex, int lastIndex) {
        if (lastIndex < firstIndex) {
            throw new ConfigException.BugOrBroken("bad call to subPath");
        }

        Path from = subPath(firstIndex);
        PathBuilder pb = new PathBuilder();
        int count = lastIndex - firstIndex;
        while (count > 0) {
            count -= 1;
            pb.appendKey(from.first());
            from = from.remainder();
            if (from == null) {
                throw new ConfigException.BugOrBroken(
                        "subPath lastIndex out of range " + lastIndex);
            }
        }
        return pb.result();
    }

    boolean startsWith(Path other) {
        Path myRemainder = this;
        Path otherRemainder = other;
        if (otherRemainder.length() <= myRemainder.length()) {
            while (otherRemainder != null) {
                if (!otherRemainder.first().equals(myRemainder.first())) {
                    return false;
                }
                myRemainder = myRemainder.remainder();
                otherRemainder = otherRemainder.remainder();
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Path) {
            Path that = (Path) other;
            return this.first.equals(that.first)
                    && ConfigImplUtil.equalsHandlingNull(this.remainder, that.remainder);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return DEFAULT_VALUE * (DEFAULT_VALUE + first.hashCode())
                + (remainder == null ? 0 : remainder.hashCode());
    }

    // this doesn't have a very precise meaning, just to reduce
    // noise from quotes in the rendered path for average cases
    static boolean hasFunkyChars(String s) {
        int length = s.length();

        if (length == 0) {
            return false;
        }

        for (int i = 0; i < length; ++i) {
            char c = s.charAt(i);

            if (Character.isLetterOrDigit(c) || c == '-' || c == '_' || c == '.') {
                continue;
            } else {
                return true;
            }
        }
        return false;
    }

    private void appendToStringBuilder(StringBuilder sb) {
        if (hasFunkyChars(first) || first.isEmpty()) {
            sb.append(ConfigImplUtil.renderJsonString(first));
        } else {
            sb.append(first);
        }
        if (remainder != null) {
            sb.append(ConfigParseOptions.PATH_TOKEN_SEPARATOR);
            remainder.appendToStringBuilder(sb);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Path(");
        appendToStringBuilder(sb);
        sb.append(")");
        return sb.toString();
    }

    /**
     * toString() is a debugging-oriented version while this is an error-message-oriented
     * human-readable one.
     */
    String render() {
        StringBuilder sb = new StringBuilder();
        appendToStringBuilder(sb);
        return sb.toString();
    }

    static Path newKey(String key) {
        return new Path(key, null);
    }

    static Path newPath(String path) {
        return PathParser.parsePath(path);
    }
}
