package io.github.interestinglab.waterdrop.config;

import io.github.interestinglab.waterdrop.config.impl.ConfigImplUtil;

import java.util.List;

/**
 * Contains static utility methods.
 * 
 */
public final class ConfigUtil {
    private ConfigUtil() {

    }

    /**
     * Quotes and escapes a string, as in the JSON specification.
     *
     * @param s
     *            a string
     * @return the string quoted and escaped
     */
    public static String quoteString(String s) {
        return ConfigImplUtil.renderJsonString(s);
    }

    /**
     * Converts a list of keys to a path expression, by quoting the path
     * elements as needed and then joining them separated by a period. A path
     * expression is usable with a {@link Config}, while individual path
     * elements are usable with a {@link ConfigObject}.
     * <p>
     * See the overview documentation for {@link Config} for more detail on path
     * expressions vs. keys.
     * 
     * @param elements
     *            the keys in the path
     * @return a path expression
     * @throws ConfigException
     *             if there are no elements
     */
    public static String joinPath(String... elements) {
        return ConfigImplUtil.joinPath(elements);
    }

    /**
     * Converts a list of strings to a path expression, by quoting the path
     * elements as needed and then joining them separated by a period. A path
     * expression is usable with a {@link Config}, while individual path
     * elements are usable with a {@link ConfigObject}.
     * <p>
     * See the overview documentation for {@link Config} for more detail on path
     * expressions vs. keys.
     * 
     * @param elements
     *            the keys in the path
     * @return a path expression
     * @throws ConfigException
     *             if the list is empty
     */
    public static String joinPath(List<String> elements) {
        return ConfigImplUtil.joinPath(elements);
    }

    /**
     * Converts a path expression into a list of keys, by splitting on period
     * and unquoting the individual path elements. A path expression is usable
     * with a {@link Config}, while individual path elements are usable with a
     * {@link ConfigObject}.
     * <p>
     * See the overview documentation for {@link Config} for more detail on path
     * expressions vs. keys.
     * 
     * @param path
     *            a path expression
     * @return the individual keys in the path
     * @throws ConfigException
     *             if the path expression is invalid
     */
    public static List<String> splitPath(String path) {
        return ConfigImplUtil.splitPath(path);
    }
}
