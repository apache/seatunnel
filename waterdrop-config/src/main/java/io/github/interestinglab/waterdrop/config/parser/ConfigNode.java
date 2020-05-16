/**
 *   Copyright (C) 2015 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.parser;

/**
 * A node in the syntax tree for a HOCON or JSON document.
 *
 * <p>
 * Note: at present there is no way to obtain an instance of this interface, so
 * please ignore it. A future release will make syntax tree nodes available in
 * the public API. If you are interested in working on it, please see: <a
 * href="https://github.com/lightbend/config/issues/300"
 * >https://github.com/lightbend/config/issues/300</a>
 *
 * <p>
 * Because this object is immutable, it is safe to use from multiple threads and
 * there's no need for "defensive copies."
 *
 * <p>
 * <em>Do not implement interface {@code ConfigNode}</em>; it should only be
 * implemented by the config library. Arbitrary implementations will not work
 * because the library internals assume a specific concrete implementation.
 * Also, this interface is likely to grow new methods over time, so third-party
 * implementations will break.
 */
public interface ConfigNode {
    /**
     * The original text of the input which was used to form this particular
     * node.
     *
     * @return the original text used to form this node as a String
     */
    public String render();
}
