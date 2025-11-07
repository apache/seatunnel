/*
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */

package org.apache.seatunnel.shade.com.typesafe.config.impl;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigIncludeContext;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigOrigin;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigSyntax;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

final class ConfigParser {
    static AbstractConfigValue parse(
            ConfigNodeRoot document,
            ConfigOrigin origin,
            ConfigParseOptions options,
            ConfigIncludeContext includeContext) {
        ParseContext context =
                new ParseContext(
                        options.getSyntax(),
                        origin,
                        document,
                        SimpleIncluder.makeFull(options.getIncluder()),
                        includeContext);
        return context.parse();
    }

    private static final class ParseContext {
        private int lineNumber;
        private final ConfigNodeRoot document;
        private final FullIncluder includer;
        private final ConfigIncludeContext includeContext;
        private final ConfigSyntax flavor;
        private final ConfigOrigin baseOrigin;
        private final LinkedList<Path> pathStack;

        // the number of lists we are inside; this is used to detect the "cannot
        // generate a reference to a list element" problem, and once we fix that
        // problem we should be able to get rid of this variable.
        int arrayCount;

        ParseContext(
                ConfigSyntax flavor,
                ConfigOrigin origin,
                ConfigNodeRoot document,
                FullIncluder includer,
                ConfigIncludeContext includeContext) {
            lineNumber = 1;
            this.document = document;
            this.flavor = flavor;
            this.baseOrigin = origin;
            this.includer = includer;
            this.includeContext = includeContext;
            this.pathStack = new LinkedList<>();
            this.arrayCount = 0;
        }

        // merge a bunch of adjacent values into one
        // value; change unquoted text into a string
        // value.
        private AbstractConfigValue parseConcatenation(ConfigNodeConcatenation n) {
            // this trick is not done in JSON
            if (flavor == ConfigSyntax.JSON) {
                throw new ConfigException.BugOrBroken("Found a concatenation node in JSON");
            }

            List<AbstractConfigValue> values = new ArrayList<>();

            for (AbstractConfigNode node : n.children()) {
                AbstractConfigValue v = null;
                if (node instanceof AbstractConfigNodeValue) {
                    v = parseValue((AbstractConfigNodeValue) node, null);
                    values.add(v);
                }
            }

            return ConfigConcatenation.concatenate(values);
        }

        private SimpleConfigOrigin lineOrigin() {
            return ((SimpleConfigOrigin) baseOrigin).withLineNumber(lineNumber);
        }

        private ConfigException parseError(String message) {
            return parseError(message, null);
        }

        private ConfigException parseError(String message, Throwable cause) {
            return new ConfigException.Parse(lineOrigin(), message, cause);
        }

        private Path fullCurrentPath() {
            // pathStack has top of stack at front
            if (pathStack.isEmpty()) {
                throw new ConfigException.BugOrBroken(
                        "Bug in parser; tried to get current path when at root");
            } else {
                return new Path(pathStack.descendingIterator());
            }
        }

        private AbstractConfigValue parseValue(AbstractConfigNodeValue n, List<String> comments) {
            AbstractConfigValue v;

            int startingArrayCount = arrayCount;

            if (n instanceof ConfigNodeSimpleValue) {
                v = ((ConfigNodeSimpleValue) n).value();
            } else if (n instanceof ConfigNodeObject) {

                Path path = pathStack.peekFirst();

                if (path != null
                        && !ConfigSyntax.JSON.equals(flavor)
                        && ("source".equals(path.first())
                                || "transform".equals(path.first())
                                || "sink".equals(path.first()))) {
                    v = parseObjectForSeaTunnel((ConfigNodeObject) n);
                } else {
                    v = parseObject((ConfigNodeObject) n);
                }

            } else if (n instanceof ConfigNodeArray) {
                v = parseArray((ConfigNodeArray) n);
            } else if (n instanceof ConfigNodeConcatenation) {
                v = parseConcatenation((ConfigNodeConcatenation) n);
            } else {
                throw parseError("Expecting a value but got wrong node type: " + n.getClass());
            }

            if (comments != null && !comments.isEmpty()) {
                v = v.withOrigin(v.origin().prependComments(new ArrayList<>(comments)));
                comments.clear();
            }

            if (arrayCount != startingArrayCount) {
                throw new ConfigException.BugOrBroken(
                        "Bug in config parser: unbalanced array count");
            }

            return v;
        }

        private static AbstractConfigObject createValueUnderPath(
                Path path, AbstractConfigValue value) {
            // for path foo.bar, we are creating
            // { "foo" : { "bar" : value } }
            List<String> keys = new ArrayList<>();

            String key = path.first();
            Path remaining = path.remainder();
            while (key != null) {
                keys.add(key);
                if (remaining == null) {
                    break;
                } else {
                    key = remaining.first();
                    remaining = remaining.remainder();
                }
            }

            // the withComments(null) is to ensure comments are only
            // on the exact leaf node they apply to.
            // a comment before "foo.bar" applies to the full setting
            // "foo.bar" not also to "foo"
            ListIterator<String> i = keys.listIterator(keys.size());
            String deepest = i.previous();
            AbstractConfigObject o =
                    new SimpleConfigObject(
                            value.origin().withComments(null),
                            Collections.singletonMap(deepest, value));
            while (i.hasPrevious()) {
                Map<String, AbstractConfigValue> m = Collections.singletonMap(i.previous(), o);
                o = new SimpleConfigObject(value.origin().withComments(null), m);
            }

            return o;
        }

        private void parseInclude(Map<String, AbstractConfigValue> values, ConfigNodeInclude n) {
            boolean isRequired = n.isRequired();
            ConfigIncludeContext cic =
                    includeContext.setParseOptions(
                            includeContext.parseOptions().setAllowMissing(!isRequired));

            AbstractConfigObject obj;
            switch (n.kind()) {
                case URL:
                    URL url;
                    try {
                        url = new URL(n.name());
                    } catch (MalformedURLException e) {
                        throw parseError("include url() specifies an invalid URL: " + n.name(), e);
                    }
                    obj = (AbstractConfigObject) includer.includeURL(cic, url);
                    break;

                case FILE:
                    obj = (AbstractConfigObject) includer.includeFile(cic, new File(n.name()));
                    break;

                case CLASSPATH:
                    obj = (AbstractConfigObject) includer.includeResources(cic, n.name());
                    break;

                case HEURISTIC:
                    obj = (AbstractConfigObject) includer.include(cic, n.name());
                    break;

                default:
                    throw new ConfigException.BugOrBroken("should not be reached");
            }

            // we really should make this work, but for now throwing an
            // exception is better than producing an incorrect result.
            // See https://github.com/lightbend/config/issues/160
            if (arrayCount > 0 && obj.resolveStatus() != ResolveStatus.RESOLVED) {
                throw parseError(
                        "Due to current limitations of the config parser, when an include statement is nested inside a list value, "
                                + "${} substitutions inside the included file cannot be resolved correctly. Either move the include outside of the list value or "
                                + "remove the ${} statements from the included file.");
            }

            if (!pathStack.isEmpty()) {
                Path prefix = fullCurrentPath();
                obj = obj.relativized(prefix);
            }

            for (String key : obj.keySet()) {
                AbstractConfigValue v = obj.get(key);
                AbstractConfigValue existing = values.get(key);
                if (existing != null) {
                    values.put(key, v.withFallback(existing));
                } else {
                    values.put(key, v);
                }
            }
        }

        private SimpleConfigList parseObjectForSeaTunnel(ConfigNodeObject n) {

            Map<String, AbstractConfigValue> values = new LinkedHashMap<>();
            List<AbstractConfigValue> valuesList = new ArrayList<>();
            SimpleConfigOrigin objectOrigin = lineOrigin();
            boolean lastWasNewline = false;

            ArrayList<AbstractConfigNode> nodes = new ArrayList<>(n.children());
            List<String> comments = new ArrayList<>();
            for (int i = 0; i < nodes.size(); i++) {
                AbstractConfigNode node = nodes.get(i);
                if (node instanceof ConfigNodeComment) {
                    lastWasNewline = false;
                    comments.add(((ConfigNodeComment) node).commentText());
                } else if (node instanceof ConfigNodeSingleToken
                        && Tokens.isNewline(((ConfigNodeSingleToken) node).token())) {
                    lineNumber++;
                    if (lastWasNewline) {
                        // Drop all comments if there was a blank line and start a new comment block
                        comments.clear();
                    }
                    lastWasNewline = true;
                } else if (flavor != ConfigSyntax.JSON && node instanceof ConfigNodeInclude) {
                    parseInclude(values, (ConfigNodeInclude) node);
                    lastWasNewline = false;
                } else if (node instanceof ConfigNodeField) {
                    lastWasNewline = false;
                    Path path = ((ConfigNodeField) node).path().value();
                    comments.addAll(((ConfigNodeField) node).comments());

                    // path must be on-stack while we parse the value
                    pathStack.push(path);
                    if (((ConfigNodeField) node).separator() == Tokens.PLUS_EQUALS) {
                        // we really should make this work, but for now throwing
                        // an exception is better than producing an incorrect
                        // result. See
                        // https://github.com/lightbend/config/issues/160
                        if (arrayCount > 0) {
                            throw parseError(
                                    "Due to current limitations of the config parser, += does not work nested inside a list. "
                                            + "+= expands to a ${} substitution and the path in ${} cannot currently refer to list elements. "
                                            + "You might be able to move the += outside of the list and then refer to it from inside the list with ${}.");
                        }

                        // because we will put it in an array after the fact so
                        // we want this to be incremented during the parseValue
                        // below in order to throw the above exception.
                        arrayCount += 1;
                    }

                    AbstractConfigNodeValue valueNode;
                    AbstractConfigValue newValue;

                    valueNode = ((ConfigNodeField) node).value();

                    // comments from the key token go to the value token
                    newValue = parseValue(valueNode, comments);

                    if (((ConfigNodeField) node).separator() == Tokens.PLUS_EQUALS) {
                        arrayCount -= 1;

                        List<AbstractConfigValue> concat = new ArrayList<>(2);
                        AbstractConfigValue previousRef =
                                new ConfigReference(
                                        newValue.origin(),
                                        new SubstitutionExpression(
                                                fullCurrentPath(), true /* optional */));
                        AbstractConfigValue list =
                                new SimpleConfigList(
                                        newValue.origin(), Collections.singletonList(newValue));
                        concat.add(previousRef);
                        concat.add(list);
                        newValue = ConfigConcatenation.concatenate(concat);
                    }

                    // Grab any trailing comments on the same line
                    if (i < nodes.size() - 1) {
                        i++;
                        while (i < nodes.size()) {
                            if (nodes.get(i) instanceof ConfigNodeComment) {
                                ConfigNodeComment comment = (ConfigNodeComment) nodes.get(i);
                                newValue =
                                        newValue.withOrigin(
                                                newValue.origin()
                                                        .appendComments(
                                                                Collections.singletonList(
                                                                        comment.commentText())));
                                break;
                            } else if (nodes.get(i) instanceof ConfigNodeSingleToken) {
                                ConfigNodeSingleToken curr = (ConfigNodeSingleToken) nodes.get(i);
                                if (curr.token() == Tokens.COMMA
                                        || Tokens.isIgnoredWhitespace(curr.token())) {
                                    // keep searching, as there could still be a comment
                                } else {
                                    i--;
                                    break;
                                }
                            } else {
                                i--;
                                break;
                            }
                            i++;
                        }
                    }

                    pathStack.pop();

                    String key = path.first();
                    Path remaining = path.remainder();

                    if (remaining == null) {

                        Map<String, String> m = Collections.singletonMap("plugin_name", key);
                        newValue = newValue.withFallback(ConfigValueFactory.fromMap(m));

                        values.put(key, newValue);
                        valuesList.add(newValue);
                    } else {
                        if (flavor == ConfigSyntax.JSON) {
                            throw new ConfigException.BugOrBroken(
                                    "somehow got multi-element path in JSON mode");
                        }

                        AbstractConfigObject obj = createValueUnderPath(remaining, newValue);

                        Map<String, String> m = Collections.singletonMap("plugin_name", key);
                        obj = obj.withFallback(ConfigValueFactory.fromMap(m));

                        values.put(key, obj);
                        valuesList.add(obj);
                    }
                }
            }

            return new SimpleConfigList(objectOrigin, valuesList);
        }

        private AbstractConfigObject parseObject(ConfigNodeObject n) {
            Map<String, AbstractConfigValue> values = new LinkedHashMap<>();
            SimpleConfigOrigin objectOrigin = lineOrigin();
            boolean lastWasNewline = false;

            ArrayList<AbstractConfigNode> nodes = new ArrayList<>(n.children());
            List<String> comments = new ArrayList<>();
            for (int i = 0; i < nodes.size(); i++) {
                AbstractConfigNode node = nodes.get(i);
                if (node instanceof ConfigNodeComment) {
                    lastWasNewline = false;
                    comments.add(((ConfigNodeComment) node).commentText());
                } else if (node instanceof ConfigNodeSingleToken
                        && Tokens.isNewline(((ConfigNodeSingleToken) node).token())) {
                    lineNumber++;
                    if (lastWasNewline) {
                        // Drop all comments if there was a blank line and start a new comment block
                        comments.clear();
                    }
                    lastWasNewline = true;
                } else if (flavor != ConfigSyntax.JSON && node instanceof ConfigNodeInclude) {
                    parseInclude(values, (ConfigNodeInclude) node);
                    lastWasNewline = false;
                } else if (node instanceof ConfigNodeField) {
                    lastWasNewline = false;
                    Path path = ((ConfigNodeField) node).path().value();
                    comments.addAll(((ConfigNodeField) node).comments());

                    // path must be on-stack while we parse the value
                    pathStack.push(path);
                    if (((ConfigNodeField) node).separator() == Tokens.PLUS_EQUALS) {
                        // we really should make this work, but for now throwing
                        // an exception is better than producing an incorrect
                        // result. See
                        // https://github.com/lightbend/config/issues/160
                        if (arrayCount > 0) {
                            throw parseError(
                                    "Due to current limitations of the config parser, += does not work nested inside a list. "
                                            + "+= expands to a ${} substitution and the path in ${} cannot currently refer to list elements. "
                                            + "You might be able to move the += outside of the list and then refer to it from inside the list with ${}.");
                        }

                        // because we will put it in an array after the fact so
                        // we want this to be incremented during the parseValue
                        // below in order to throw the above exception.
                        arrayCount += 1;
                    }

                    AbstractConfigNodeValue valueNode;
                    AbstractConfigValue newValue;

                    valueNode = ((ConfigNodeField) node).value();

                    // comments from the key token go to the value token
                    newValue = parseValue(valueNode, comments);

                    if (((ConfigNodeField) node).separator() == Tokens.PLUS_EQUALS) {
                        arrayCount -= 1;

                        List<AbstractConfigValue> concat = new ArrayList<>(2);
                        AbstractConfigValue previousRef =
                                new ConfigReference(
                                        newValue.origin(),
                                        new SubstitutionExpression(
                                                fullCurrentPath(), true /* optional */));
                        AbstractConfigValue list =
                                new SimpleConfigList(
                                        newValue.origin(), Collections.singletonList(newValue));
                        concat.add(previousRef);
                        concat.add(list);
                        newValue = ConfigConcatenation.concatenate(concat);
                    }

                    // Grab any trailing comments on the same line
                    if (i < nodes.size() - 1) {
                        i++;
                        while (i < nodes.size()) {
                            if (nodes.get(i) instanceof ConfigNodeComment) {
                                ConfigNodeComment comment = (ConfigNodeComment) nodes.get(i);
                                newValue =
                                        newValue.withOrigin(
                                                newValue.origin()
                                                        .appendComments(
                                                                Collections.singletonList(
                                                                        comment.commentText())));
                                break;
                            } else if (nodes.get(i) instanceof ConfigNodeSingleToken) {
                                ConfigNodeSingleToken curr = (ConfigNodeSingleToken) nodes.get(i);
                                if (curr.token() == Tokens.COMMA
                                        || Tokens.isIgnoredWhitespace(curr.token())) {
                                    // keep searching, as there could still be a comment
                                } else {
                                    i--;
                                    break;
                                }
                            } else {
                                i--;
                                break;
                            }
                            i++;
                        }
                    }

                    pathStack.pop();

                    String key = path.first();
                    Path remaining = path.remainder();

                    if (remaining == null) {
                        AbstractConfigValue existing = values.get(key);
                        if (existing != null) {
                            // In strict JSON, dups should be an error; while in
                            // our custom config language, they should be merged
                            // if the value is an object (or substitution that
                            // could become an object).

                            if (flavor == ConfigSyntax.JSON) {
                                throw parseError(
                                        "JSON does not allow duplicate fields: '"
                                                + key
                                                + "' was already seen at "
                                                + existing.origin().description());
                            } else {
                                newValue = newValue.withFallback(existing);
                            }
                        }
                        values.put(key, newValue);
                    } else {
                        if (flavor == ConfigSyntax.JSON) {
                            throw new ConfigException.BugOrBroken(
                                    "somehow got multi-element path in JSON mode");
                        }

                        AbstractConfigObject obj = createValueUnderPath(remaining, newValue);
                        AbstractConfigValue existing = values.get(key);
                        if (existing != null) {
                            obj = obj.withFallback(existing);
                        }
                        values.put(key, obj);
                    }
                }
            }

            return new SimpleConfigObject(objectOrigin, values);
        }

        private SimpleConfigList parseArray(ConfigNodeArray n) {
            arrayCount += 1;

            SimpleConfigOrigin arrayOrigin = lineOrigin();
            List<AbstractConfigValue> values = new ArrayList<>();

            boolean lastWasNewLine = false;
            List<String> comments = new ArrayList<>();

            AbstractConfigValue v = null;

            for (AbstractConfigNode node : n.children()) {
                if (node instanceof ConfigNodeComment) {
                    comments.add(((ConfigNodeComment) node).commentText());
                    lastWasNewLine = false;
                } else if (node instanceof ConfigNodeSingleToken
                        && Tokens.isNewline(((ConfigNodeSingleToken) node).token())) {
                    lineNumber++;
                    if (lastWasNewLine && v == null) {
                        comments.clear();
                    } else if (v != null) {
                        values.add(
                                v.withOrigin(v.origin().appendComments(new ArrayList<>(comments))));
                        comments.clear();
                        v = null;
                    }
                    lastWasNewLine = true;
                } else if (node instanceof AbstractConfigNodeValue) {
                    lastWasNewLine = false;
                    if (v != null) {
                        values.add(
                                v.withOrigin(v.origin().appendComments(new ArrayList<>(comments))));
                        comments.clear();
                    }
                    v = parseValue((AbstractConfigNodeValue) node, comments);
                }
            }
            // There shouldn't be any comments at this point, but add them just in case
            if (v != null) {
                values.add(v.withOrigin(v.origin().appendComments(new ArrayList<>(comments))));
            }
            arrayCount -= 1;
            return new SimpleConfigList(arrayOrigin, values);
        }

        AbstractConfigValue parse() {
            AbstractConfigValue result = null;
            ArrayList<String> comments = new ArrayList<>();
            boolean lastWasNewLine = false;
            for (AbstractConfigNode node : document.children()) {
                if (node instanceof ConfigNodeComment) {
                    comments.add(((ConfigNodeComment) node).commentText());
                    lastWasNewLine = false;
                } else if (node instanceof ConfigNodeSingleToken) {
                    Token t = ((ConfigNodeSingleToken) node).token();
                    if (Tokens.isNewline(t)) {
                        lineNumber++;
                        if (lastWasNewLine && result == null) {
                            comments.clear();
                        } else if (result != null) {
                            result =
                                    result.withOrigin(
                                            result.origin()
                                                    .appendComments(new ArrayList<>(comments)));
                            comments.clear();
                            break;
                        }
                        lastWasNewLine = true;
                    }
                } else if (node instanceof ConfigNodeComplexValue) {
                    result = parseValue((ConfigNodeComplexValue) node, comments);
                    lastWasNewLine = false;
                }
            }
            return result;
        }
    }
}
