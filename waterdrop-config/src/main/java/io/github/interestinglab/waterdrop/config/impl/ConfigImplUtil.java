/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigException;
import io.github.interestinglab.waterdrop.config.ConfigOrigin;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Internal implementation detail, not ABI stable, do not touch.
 * For use only by the {@link io.github.interestinglab.waterdrop.config} package.
 */
final public class ConfigImplUtil {
    static boolean equalsHandlingNull(Object a, Object b) {
        if (a == null && b != null)
            return false;
        else if (a != null && b == null)
            return false;
        else if (a == b) // catches null == null plus optimizes identity case
            return true;
        else
            return a.equals(b);
    }

    static boolean isC0Control(int codepoint) {
      return (codepoint >= 0x0000 && codepoint <= 0x001F);
    }

    public static String renderJsonString(String s) {
        StringBuilder sb = new StringBuilder();
        sb.append('"');
        for (int i = 0; i < s.length(); ++i) {
            char c = s.charAt(i);
            switch (c) {
            case '"':
                sb.append("\\\"");
                break;
            case '\\':
                sb.append("\\\\");
                break;
            case '\n':
                sb.append("\\n");
                break;
            case '\b':
                sb.append("\\b");
                break;
            case '\f':
                sb.append("\\f");
                break;
            case '\r':
                sb.append("\\r");
                break;
            case '\t':
                sb.append("\\t");
                break;
            default:
                if (isC0Control(c))
                    sb.append(String.format("\\u%04x", (int) c));
                else
                    sb.append(c);
            }
        }
        sb.append('"');
        return sb.toString();
    }

    static String renderStringUnquotedIfPossible(String s) {
        // this can quote unnecessarily as long as it never fails to quote when
        // necessary
        if (s.length() == 0)
            return renderJsonString(s);

        // if it starts with a hyphen or number, we have to quote
        // to ensure we end up with a string and not a number
        int first = s.codePointAt(0);
        if (Character.isDigit(first) || first == '-')
            return renderJsonString(s);

        if (s.startsWith("include") || s.startsWith("true") || s.startsWith("false")
                || s.startsWith("null") || s.contains("//"))
            return renderJsonString(s);

        // only unquote if it's pure alphanumeric
        for (int i = 0; i < s.length(); ++i) {
            char c = s.charAt(i);
            if (!(Character.isLetter(c) || Character.isDigit(c) || c == '-'))
                return renderJsonString(s);
        }

        return s;
    }

    static boolean isWhitespace(int codepoint) {
        switch (codepoint) {
        // try to hit the most common ASCII ones first, then the nonbreaking
        // spaces that Java brokenly leaves out of isWhitespace.
        case ' ':
        case '\n':
        case '\u00A0':
        case '\u2007':
        case '\u202F':
            // this one is the BOM, see
            // http://www.unicode.org/faq/utf_bom.html#BOM
            // we just accept it as a zero-width nonbreaking space.
        case '\uFEFF':
            return true;
        default:
            return Character.isWhitespace(codepoint);
        }
    }

    public static String unicodeTrim(String s) {
        // this is dumb because it looks like there aren't any whitespace
        // characters that need surrogate encoding. But, points for
        // pedantic correctness! It's future-proof or something.
        // String.trim() actually is broken, since there are plenty of
        // non-ASCII whitespace characters.
        final int length = s.length();
        if (length == 0)
            return s;

        int start = 0;
        while (start < length) {
            char c = s.charAt(start);
            if (c == ' ' || c == '\n') {
                start += 1;
            } else {
                int cp = s.codePointAt(start);
                if (isWhitespace(cp))
                    start += Character.charCount(cp);
                else
                    break;
            }
        }

        int end = length;
        while (end > start) {
            char c = s.charAt(end - 1);
            if (c == ' ' || c == '\n') {
                --end;
            } else {
                int cp;
                int delta;
                if (Character.isLowSurrogate(c)) {
                    cp = s.codePointAt(end - 2);
                    delta = 2;
                } else {
                    cp = s.codePointAt(end - 1);
                    delta = 1;
                }
                if (isWhitespace(cp))
                    end -= delta;
                else
                    break;
            }
        }
        return s.substring(start, end);
    }


    public static ConfigException extractInitializerError(ExceptionInInitializerError e) {
        Throwable cause = e.getCause();
        if (cause != null && cause instanceof ConfigException) {
            return (ConfigException) cause;
        } else {
            throw e;
        }
    }

    static File urlToFile(URL url) {
        // this isn't really right, clearly, but not sure what to do.
        try {
            // this will properly handle hex escapes, etc.
            return new File(url.toURI());
        } catch (URISyntaxException e) {
            // this handles some stuff like file:///c:/Whatever/
            // apparently but mangles handling of hex escapes
            return new File(url.getPath());
        } catch (IllegalArgumentException e) {
            // file://foo with double slash causes
            // IllegalArgumentException "url has an authority component"
            return new File(url.getPath());
        }
    }

    public static String joinPath(String... elements) {
        return (new Path(elements)).render();
    }

    public static String joinPath(List<String> elements) {
        return joinPath(elements.toArray(new String[0]));
    }

    public static List<String> splitPath(String path) {
        Path p = Path.newPath(path);
        List<String> elements = new ArrayList<String>();
        while (p != null) {
            elements.add(p.first());
            p = p.remainder();
        }
        return elements;
    }

    public static ConfigOrigin readOrigin(ObjectInputStream in) throws IOException {
        return SerializedConfigValue.readOrigin(in, null);
    }

    public static void writeOrigin(ObjectOutputStream out, ConfigOrigin origin) throws IOException {
        SerializedConfigValue.writeOrigin(new DataOutputStream(out), (SimpleConfigOrigin) origin,
                null);
    }

    static String toCamelCase(String originalName) {
        String[] words = originalName.split("-+");
        StringBuilder nameBuilder = new StringBuilder(originalName.length());
        for (String word : words) {
            if (nameBuilder.length() == 0) {
                nameBuilder.append(word);
            } else {
                nameBuilder.append(word.substring(0, 1).toUpperCase());
                nameBuilder.append(word.substring(1));
            }
        }
        return nameBuilder.toString();
    }
}
