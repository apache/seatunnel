/**
 *   Copyright (C) 2015 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.*;
import io.github.interestinglab.waterdrop.config.parser.*;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


final class PathParser {
    static class Element {
        StringBuilder sb;
        // an element can be empty if it has a quoted empty string "" in it
        boolean canBeEmpty;

        Element(String initial, boolean canBeEmpty) {
            this.canBeEmpty = canBeEmpty;
            this.sb = new StringBuilder(initial);
        }

        @Override
        public String toString() {
            return "Element(" + sb.toString() + "," + canBeEmpty + ")";
        }
    }

    static ConfigOrigin apiOrigin = SimpleConfigOrigin.newSimple("path parameter");

    static ConfigNodePath parsePathNode(String path) {
        return parsePathNode(path, ConfigSyntax.CONF);
    }

    static ConfigNodePath parsePathNode(String path, ConfigSyntax flavor) {
        StringReader reader = new StringReader(path);

        try {
            Iterator<Token> tokens = Tokenizer.tokenize(apiOrigin, reader,
                    flavor);
            tokens.next(); // drop START
            return parsePathNodeExpression(tokens, apiOrigin, path, flavor);
        } finally {
            reader.close();
        }
    }

    static Path parsePath(String path) {
        Path speculated = speculativeFastParsePath(path);
        if (speculated != null)
            return speculated;

        StringReader reader = new StringReader(path);

        try {
            Iterator<Token> tokens = Tokenizer.tokenize(apiOrigin, reader,
                    ConfigSyntax.CONF);
            tokens.next(); // drop START
            return parsePathExpression(tokens, apiOrigin, path);
        } finally {
            reader.close();
        }
    }

    protected static Path parsePathExpression(Iterator<Token> expression,
                                              ConfigOrigin origin) {
        return parsePathExpression(expression, origin, null, null, ConfigSyntax.CONF);
    }

    protected static Path parsePathExpression(Iterator<Token> expression,
                                              ConfigOrigin origin, String originalText) {
        return parsePathExpression(expression, origin, originalText, null, ConfigSyntax.CONF);
    }

    protected static ConfigNodePath parsePathNodeExpression(Iterator<Token> expression,
                                                            ConfigOrigin origin) {
        return parsePathNodeExpression(expression, origin, null, ConfigSyntax.CONF);
    }

    protected static ConfigNodePath parsePathNodeExpression(Iterator<Token> expression,
                                                            ConfigOrigin origin, String originalText, ConfigSyntax flavor) {
        ArrayList<Token> pathTokens = new ArrayList<Token>();
        Path path = parsePathExpression(expression, origin, originalText, pathTokens, flavor);
        return new ConfigNodePath(path, pathTokens);
    }

    // originalText may be null if not available
    protected static Path parsePathExpression(Iterator<Token> expression,
                                              ConfigOrigin origin, String originalText,
                                              ArrayList<Token> pathTokens,
                                              ConfigSyntax flavor) {
        // each builder in "buf" is an element in the path.
        List<Element> buf = new ArrayList<Element>();
        buf.add(new Element("", false));

        if (!expression.hasNext()) {
            throw new ConfigException.BadPath(origin, originalText,
                    "Expecting a field name or path here, but got nothing");
        }

        while (expression.hasNext()) {
            Token t = expression.next();

            if (pathTokens != null)
                pathTokens.add(t);

            // Ignore all IgnoredWhitespace tokens
            if (Tokens.isIgnoredWhitespace(t))
                continue;

            if (Tokens.isValueWithType(t, ConfigValueType.STRING)) {
                AbstractConfigValue v = Tokens.getValue(t);
                // this is a quoted string; so any periods
                // in here don't count as path separators
                String s = v.transformToString();

                addPathText(buf, true, s);
            } else if (t == Tokens.END) {
                // ignore this; when parsing a file, it should not happen
                // since we're parsing a token list rather than the main
                // token iterator, and when parsing a path expression from the
                // API, it's expected to have an END.
            } else {
                // any periods outside of a quoted string count as
                // separators
                String text;
                if (Tokens.isValue(t)) {
                    // appending a number here may add
                    // a period, but we _do_ count those as path
                    // separators, because we basically want
                    // "foo 3.0bar" to parse as a string even
                    // though there's a number in it. The fact that
                    // we tokenize non-string values is largely an
                    // implementation detail.
                    AbstractConfigValue v = Tokens.getValue(t);

                    // We need to split the tokens on a . so that we can get sub-paths but still preserve
                    // the original path text when doing an insertion
                    if (pathTokens != null) {
                        pathTokens.remove(pathTokens.size() - 1);
                        pathTokens.addAll(splitTokenOnPeriod(t, flavor));
                    }
                    text = v.transformToString();
                } else if (Tokens.isUnquotedText(t)) {
                    // We need to split the tokens on a . so that we can get sub-paths but still preserve
                    // the original path text when doing an insertion on ConfigNodeObjects
                    if (pathTokens != null) {
                        pathTokens.remove(pathTokens.size() - 1);
                        pathTokens.addAll(splitTokenOnPeriod(t, flavor));
                    }
                    text = Tokens.getUnquotedText(t);
                } else {
                    throw new ConfigException.BadPath(
                            origin,
                            originalText,
                            "Token not allowed in path expression: "
                                    + t
                                    + " (you can double-quote this token if you really want it here)");
                }

                addPathText(buf, false, text);
            }
        }

        PathBuilder pb = new PathBuilder();
        for (Element e : buf) {
            if (e.sb.length() == 0 && !e.canBeEmpty) {
                throw new ConfigException.BadPath(
                        origin,
                        originalText,
                        "path has a leading, trailing, or two adjacent period '.' (use quoted \"\" empty string if you want an empty element)");
            } else {
                pb.appendKey(e.sb.toString());
            }
        }

        return pb.result();
    }

    private static Collection<Token> splitTokenOnPeriod(Token t, ConfigSyntax flavor) {

        String tokenText = t.tokenText();
        if (tokenText.equals(ConfigParseOptions.pathTokenSeparator)) {
            return Collections.singletonList(t);
        }
        String[] splitToken = tokenText.split(ConfigParseOptions.pathTokenSeparator);
        ArrayList<Token> splitTokens = new ArrayList<Token>();
        for (String s : splitToken) {
            if (flavor == ConfigSyntax.CONF)
                splitTokens.add(Tokens.newUnquotedText(t.origin(), s));
            else
                splitTokens.add(Tokens.newString(t.origin(), s, "\"" + s + "\""));
            splitTokens.add(Tokens.newUnquotedText(t.origin(), ConfigParseOptions.pathTokenSeparator));
        }

        if (! tokenText.substring(tokenText.length() - ConfigParseOptions.pathTokenSeparator.length(), tokenText.length()).equals(ConfigParseOptions.pathTokenSeparator)) {
            splitTokens.remove(splitTokens.size() - 1);
        }

        return splitTokens;
    }

    private static void addPathText(List<Element> buf, boolean wasQuoted,
                                    String newText) {

        int i = wasQuoted ? -1 : newText.indexOf(ConfigParseOptions.pathTokenSeparator);
        Element current = buf.get(buf.size() - 1);
        if (i < 0) {
            // add to current path element
            current.sb.append(newText);
            // any empty quoted string means this element can
            // now be empty.
            if (wasQuoted && current.sb.length() == 0)
                current.canBeEmpty = true;
        } else {
            // "buf" plus up to the period is an element
            current.sb.append(newText.substring(0, i));
            // then start a new element
            buf.add(new Element("", false));
            // recurse to consume remainder of newText
            addPathText(buf, false, newText.substring(i + ConfigParseOptions.pathTokenSeparator.length()));
        }
    }

    // the idea is to see if the string has any chars or features
    // that might require the full parser to deal with.
    private static boolean looksUnsafeForFastParser(String s) {
        // TODO: maybe we should rewrite this function using ConfigParseOptions.pathTokenSeparator
        boolean lastWasDot = true; // start of path is also a "dot"
        int len = s.length();
        if (s.isEmpty())
            return true;
        if (s.charAt(0) == '.')
            return true;
        if (s.charAt(len - 1) == '.')
            return true;

        for (int i = 0; i < len; ++i) {
            char c = s.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
                lastWasDot = false;
                continue;
            } else if (c == '.') {
                if (lastWasDot)
                    return true; // ".." means we need to throw an error
                lastWasDot = true;
            } else if (c == '-') {
                if (lastWasDot)
                    return true;
                continue;
            } else {
                return true;
            }
        }

        if (lastWasDot)
            return true;

        return false;
    }

    private static Path fastPathBuild(Path tail, String s, int end) {

        // lastIndexOf takes last index it should look at, end - 1 not end
        int splitAt = s.lastIndexOf(ConfigParseOptions.pathTokenSeparator,end - 1);
        ArrayList<Token> tokens = new ArrayList<Token>();
        tokens.add(Tokens.newUnquotedText(null, s));
        // this works even if splitAt is -1; then we start the substring at 0

        if (splitAt < 0) {
            Path withOneMoreElement = new Path(s.substring(0, end), tail);
            return withOneMoreElement;
        } else {
            Path withOneMoreElement = new Path(s.substring(splitAt + ConfigParseOptions.pathTokenSeparator.length(), end), tail);
            return fastPathBuild(withOneMoreElement, s, splitAt);
        }
    }

    // do something much faster than the full parser if
    // we just have something like "foo" or "foo.bar"
    private static Path speculativeFastParsePath(String path) {
        String s = ConfigImplUtil.unicodeTrim(path);
        if (looksUnsafeForFastParser(s))
            return null;

        return fastPathBuild(null, s, s.length());
    }
}
