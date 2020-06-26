/**
 *   Copyright (C) 2015 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

final class ConfigDocumentParser {
    static ConfigNodeRoot parse(Iterator<Token> tokens, ConfigOrigin origin, ConfigParseOptions options) {
        ConfigSyntax syntax = options.getSyntax() == null ? ConfigSyntax.CONF : options.getSyntax();
        ParseContext context = new ParseContext(syntax, origin, tokens);
        return context.parse();
    }

    static AbstractConfigNodeValue parseValue(Iterator<Token> tokens, ConfigOrigin origin, ConfigParseOptions options) {
        ConfigSyntax syntax = options.getSyntax() == null ? ConfigSyntax.CONF : options.getSyntax();
        ParseContext context = new ParseContext(syntax, origin, tokens);
        return context.parseSingleValue();
    }

    static private final class ParseContext {
        private int lineNumber;
        final private Stack<Token> buffer;
        final private Iterator<Token> tokens;
        final private ConfigSyntax flavor;
        final private ConfigOrigin baseOrigin;
        // this is the number of "equals" we are inside,
        // used to modify the error message to reflect that
        // someone may think this is .properties format.
        int equalsCount;

        ParseContext(ConfigSyntax flavor, ConfigOrigin origin, Iterator<Token> tokens) {
            lineNumber = 1;
            buffer = new Stack<Token>();
            this.tokens = tokens;
            this.flavor = flavor;
            this.equalsCount = 0;
            this.baseOrigin = origin;
        }

        private Token popToken() {
            if (buffer.isEmpty()) {
                return tokens.next();
            }
            return buffer.pop();
        }

        private Token nextToken() {
            Token t = popToken();
            if (flavor == ConfigSyntax.JSON) {
                if (Tokens.isUnquotedText(t) && !isUnquotedWhitespace(t)) {
                    throw parseError("Token not allowed in valid JSON: '"
                            + Tokens.getUnquotedText(t) + "'");
                } else if (Tokens.isSubstitution(t)) {
                    throw parseError("Substitutions (${} syntax) not allowed in JSON");
                }
            }
            return t;
        }

        private Token nextTokenCollectingWhitespace(Collection<AbstractConfigNode> nodes) {
            while (true) {
                Token t = nextToken();
                if (Tokens.isIgnoredWhitespace(t) || Tokens.isNewline(t) || isUnquotedWhitespace(t)) {
                    nodes.add(new ConfigNodeSingleToken(t));
                    if (Tokens.isNewline(t)) {
                        lineNumber = t.lineNumber() + 1;
                    }
                } else if (Tokens.isComment(t)) {
                    nodes.add(new ConfigNodeComment(t));
                } else {
                    int newNumber = t.lineNumber();
                    if (newNumber >= 0)
                        lineNumber = newNumber;
                    return t;
                }
            }
        }

        private void putBack(Token token) {
            buffer.push(token);
        }

        // In arrays and objects, comma can be omitted
        // as long as there's at least one newline instead.
        // this skips any newlines in front of a comma,
        // skips the comma, and returns true if it found
        // either a newline or a comma. The iterator
        // is left just after the comma or the newline.
        private boolean checkElementSeparator(Collection<AbstractConfigNode> nodes) {
            if (flavor == ConfigSyntax.JSON) {
                Token t = nextTokenCollectingWhitespace(nodes);
                if (t == Tokens.COMMA) {
                    nodes.add(new ConfigNodeSingleToken(t));
                    return true;
                } else {
                    putBack(t);
                    return false;
                }
            } else {
                boolean sawSeparatorOrNewline = false;
                Token t = nextToken();
                while (true) {
                    if (Tokens.isIgnoredWhitespace(t) || isUnquotedWhitespace(t)) {
                        nodes.add(new ConfigNodeSingleToken(t));
                    } else if (Tokens.isComment(t)) {
                        nodes.add(new ConfigNodeComment(t));
                    } else if (Tokens.isNewline(t)) {
                        sawSeparatorOrNewline = true;
                        lineNumber++;
                        nodes.add(new ConfigNodeSingleToken(t));
                        // we want to continue to also eat
                        // a comma if there is one.
                    } else if (t == Tokens.COMMA) {
                        nodes.add(new ConfigNodeSingleToken(t));
                        return true;
                    } else {
                        // non-newline-or-comma
                        putBack(t);
                        return sawSeparatorOrNewline;
                    }
                    t = nextToken();
                }
            }
        }

        // parse a concatenation. If there is no concatenation, return the next value
        private AbstractConfigNodeValue consolidateValues(Collection<AbstractConfigNode> nodes) {
            // this trick is not done in JSON
            if (flavor == ConfigSyntax.JSON)
                return null;

            // create only if we have value tokens
            ArrayList<AbstractConfigNode> values = new ArrayList<AbstractConfigNode>();
            int valueCount = 0;

            // ignore a newline up front
            Token t = nextTokenCollectingWhitespace(nodes);
            while (true) {
                AbstractConfigNodeValue v = null;
                if (Tokens.isIgnoredWhitespace(t)) {
                    values.add(new ConfigNodeSingleToken(t));
                    t = nextToken();
                    continue;
                }
                else if (Tokens.isValue(t) || Tokens.isUnquotedText(t)
                        || Tokens.isSubstitution(t) || t == Tokens.OPEN_CURLY
                        || t == Tokens.OPEN_SQUARE) {
                    // there may be newlines _within_ the objects and arrays
                    v = parseValue(t);
                    valueCount++;
                } else {
                    break;
                }

                if (v == null)
                    throw new ConfigException.BugOrBroken("no value");

                values.add(v);

                t = nextToken(); // but don't consolidate across a newline
            }

            putBack(t);

            // No concatenation was seen, but a single value may have been parsed, so return it, and put back
            // all succeeding tokens
            if (valueCount < 2) {
                AbstractConfigNodeValue value = null;
                for (AbstractConfigNode node : values) {
                    if (node instanceof AbstractConfigNodeValue)
                        value = (AbstractConfigNodeValue)node;
                    else if (value == null)
                        nodes.add(node);
                    else
                        putBack((new ArrayList<Token>(node.tokens())).get(0));
                }
                return value;
            }

            // Put back any trailing whitespace, as the parent object is responsible for tracking
            // any leading/trailing whitespace
            for (int i = values.size() - 1; i >= 0; i--) {
                if (values.get(i) instanceof ConfigNodeSingleToken) {
                    putBack(((ConfigNodeSingleToken) values.get(i)).token());
                    values.remove(i);
                } else {
                    break;
                }
            }
            return new ConfigNodeConcatenation(values);
        }

        private ConfigException parseError(String message) {
            return parseError(message, null);
        }

        private ConfigException parseError(String message, Throwable cause) {
            return new ConfigException.Parse(baseOrigin.withLineNumber(lineNumber), message, cause);
        }

        private String addQuoteSuggestion(String badToken, String message) {
            return addQuoteSuggestion(null, equalsCount > 0, badToken, message);
        }

        private String addQuoteSuggestion(Path lastPath, boolean insideEquals, String badToken,
                                          String message) {
            String previousFieldName = lastPath != null ? lastPath.render() : null;

            String part;
            if (badToken.equals(Tokens.END.toString())) {
                // EOF requires special handling for the error to make sense.
                if (previousFieldName != null)
                    part = message + " (if you intended '" + previousFieldName
                            + "' to be part of a value, instead of a key, "
                            + "try adding double quotes around the whole value";
                else
                    return message;
            } else {
                if (previousFieldName != null) {
                    part = message + " (if you intended " + badToken
                            + " to be part of the value for '" + previousFieldName + "', "
                            + "try enclosing the value in double quotes";
                } else {
                    part = message + " (if you intended " + badToken
                            + " to be part of a key or string value, "
                            + "try enclosing the key or value in double quotes";
                }
            }

            if (insideEquals)
                return part
                        + ", or you may be able to rename the file .properties rather than .conf)";
            else
                return part + ")";
        }

        private AbstractConfigNodeValue parseValue(Token t) {
            AbstractConfigNodeValue v = null;
            int startingEqualsCount = equalsCount;

            if (Tokens.isValue(t) || Tokens.isUnquotedText(t) || Tokens.isSubstitution(t)) {
                v = new ConfigNodeSimpleValue(t);
            } else if (t == Tokens.OPEN_CURLY) {
                v = parseObject(true);
            } else if (t== Tokens.OPEN_SQUARE) {
                v = parseArray();
            } else {
                throw parseError(addQuoteSuggestion(t.toString(),
                        "Expecting a value but got wrong token: " + t));
            }

            if (equalsCount != startingEqualsCount)
                throw new ConfigException.BugOrBroken("Bug in config parser: unbalanced equals count");

            return v;
        }

        private ConfigNodePath parseKey(Token token) {
            if (flavor == ConfigSyntax.JSON) {
                if (Tokens.isValueWithType(token, ConfigValueType.STRING)) {
                    return PathParser.parsePathNodeExpression(Collections.singletonList(token).iterator(),
                                                              baseOrigin.withLineNumber(lineNumber));
                } else {
                    throw parseError("Expecting close brace } or a field name here, got "
                            + token);
                }
            } else {
                List<Token> expression = new ArrayList<Token>();
                Token t = token;
                while (Tokens.isValue(t) || Tokens.isUnquotedText(t)) {
                    expression.add(t);
                    t = nextToken(); // note: don't cross a newline
                }

                if (expression.isEmpty()) {
                    throw parseError(ExpectingClosingParenthesisError + t);
                }

                putBack(t); // put back the token we ended with
                return PathParser.parsePathNodeExpression(expression.iterator(),
                                                          baseOrigin.withLineNumber(lineNumber));
            }
        }

        private static boolean isIncludeKeyword(Token t) {
            return Tokens.isUnquotedText(t)
                    && Tokens.getUnquotedText(t).equals("include");
        }

        private static boolean isUnquotedWhitespace(Token t) {
            if (!Tokens.isUnquotedText(t))
                return false;

            String s = Tokens.getUnquotedText(t);

            for (int i = 0; i < s.length(); ++i) {
                char c = s.charAt(i);
                if (!ConfigImplUtil.isWhitespace(c))
                    return false;
            }
            return true;
        }

        private boolean isKeyValueSeparatorToken(Token t) {
            if (flavor == ConfigSyntax.JSON) {
                return t == Tokens.COLON;
            } else {
                return t == Tokens.COLON || t == Tokens.EQUALS || t == Tokens.PLUS_EQUALS;
            }
        }

        private final String ExpectingClosingParenthesisError = "expecting a close parentheses ')' here, not: ";

        private ConfigNodeInclude parseInclude(ArrayList<AbstractConfigNode> children) {

            Token t = nextTokenCollectingWhitespace(children);

            // we either have a 'required()' or a quoted string or the "file()" syntax
            if (Tokens.isUnquotedText(t)) {
                String kindText = Tokens.getUnquotedText(t);

                if (kindText.startsWith("required(")) {
                    String r = kindText.replaceFirst("required\\(","");
                    if (r.length()>0) {
                        putBack(Tokens.newUnquotedText(t.origin(),r));
                    }

                    children.add(new ConfigNodeSingleToken(t));
                    //children.add(new ConfigNodeSingleToken(tOpen));

                    ConfigNodeInclude res = parseIncludeResource(children, true);

                    t = nextTokenCollectingWhitespace(children);

                    if (Tokens.isUnquotedText(t) && Tokens.getUnquotedText(t).equals(")")) {
                        // OK, close paren
                    } else {
                        throw parseError(ExpectingClosingParenthesisError + t);
                    }

                    return res;
                } else {
                    putBack(t);
                    return parseIncludeResource(children, false);
                }
            }
            else {
                putBack(t);
                return parseIncludeResource(children, false);
            }
        }

        private ConfigNodeInclude parseIncludeResource(ArrayList<AbstractConfigNode> children, boolean isRequired) {
            Token t = nextTokenCollectingWhitespace(children);

            // we either have a quoted string or the "file()" syntax
            if (Tokens.isUnquotedText(t)) {
                // get foo(
                String kindText = Tokens.getUnquotedText(t);
                ConfigIncludeKind kind;
                String prefix;

                if (kindText.startsWith("url(")) {
                    kind = ConfigIncludeKind.URL;
                    prefix = "url(";
                } else if (kindText.startsWith("file(")) {
                    kind = ConfigIncludeKind.FILE;
                    prefix = "file(";
                } else if (kindText.startsWith("classpath(")) {
                    kind = ConfigIncludeKind.CLASSPATH;
                    prefix = "classpath(";
                } else {
                    throw parseError("expecting include parameter to be quoted filename, file(), classpath(), or url(). No spaces are allowed before the open paren. Not expecting: "
                            + t);
                }
                String r = kindText.replaceFirst("[^(]*\\(","");
                if (r.length()>0) {
                    putBack(Tokens.newUnquotedText(t.origin(),r));
                }

                children.add(new ConfigNodeSingleToken(t));

                // skip space inside parens
                t = nextTokenCollectingWhitespace(children);

                // quoted string
                if (!Tokens.isValueWithType(t, ConfigValueType.STRING)) {
                    throw parseError("expecting include " + prefix + ") parameter to be a quoted string, rather than: " + t);
                }
                children.add(new ConfigNodeSimpleValue(t));
                // skip space after string, inside parens
                t = nextTokenCollectingWhitespace(children);

                if (Tokens.isUnquotedText(t) && Tokens.getUnquotedText(t).startsWith(")")) {
                    String rest = Tokens.getUnquotedText(t).substring(1);
                    if (rest.length()>0) {
                        putBack(Tokens.newUnquotedText(t.origin(),rest));
                    }
                    // OK, close paren
                } else {
                    throw parseError(ExpectingClosingParenthesisError + t);
                }

                return new ConfigNodeInclude(children, kind, isRequired);
            } else if (Tokens.isValueWithType(t, ConfigValueType.STRING)) {
                children.add(new ConfigNodeSimpleValue(t));
                return new ConfigNodeInclude(children, ConfigIncludeKind.HEURISTIC, isRequired);
            } else {
                throw parseError("include keyword is not followed by a quoted string, but by: " + t);
            }
        }

        private ConfigNodeComplexValue parseObject(boolean hadOpenCurly) {
            // invoked just after the OPEN_CURLY (or START, if !hadOpenCurly)
            boolean afterComma = false;
            Path lastPath = null;
            boolean lastInsideEquals = false;
            ArrayList<AbstractConfigNode> objectNodes = new ArrayList<AbstractConfigNode>();
            ArrayList<AbstractConfigNode> keyValueNodes;
            HashMap<String, Boolean> keys  = new HashMap<String, Boolean>();
            if (hadOpenCurly)
                objectNodes.add(new ConfigNodeSingleToken(Tokens.OPEN_CURLY));

            while (true) {
                Token t = nextTokenCollectingWhitespace(objectNodes);
                if (t == Tokens.CLOSE_CURLY) {
                    if (flavor == ConfigSyntax.JSON && afterComma) {
                        throw parseError(addQuoteSuggestion(t.toString(),
                                "expecting a field name after a comma, got a close brace } instead"));
                    } else if (!hadOpenCurly) {
                        throw parseError(addQuoteSuggestion(t.toString(),
                                "unbalanced close brace '}' with no open brace"));
                    }
                    objectNodes.add(new ConfigNodeSingleToken(Tokens.CLOSE_CURLY));
                    break;
                } else if (t == Tokens.END && !hadOpenCurly) {
                    putBack(t);
                    break;
                } else if (flavor != ConfigSyntax.JSON && isIncludeKeyword(t)) {
                    ArrayList<AbstractConfigNode> includeNodes = new ArrayList<AbstractConfigNode>();
                    includeNodes.add(new ConfigNodeSingleToken(t));
                    objectNodes.add(parseInclude(includeNodes));
                    afterComma = false;
                } else {
                    keyValueNodes = new ArrayList<AbstractConfigNode>();
                    Token keyToken = t;
                    ConfigNodePath path = parseKey(keyToken);
                    keyValueNodes.add(path);
                    Token afterKey = nextTokenCollectingWhitespace(keyValueNodes);
                    boolean insideEquals = false;

                    AbstractConfigNodeValue nextValue;
                    if (flavor == ConfigSyntax.CONF && afterKey == Tokens.OPEN_CURLY) {
                        // can omit the ':' or '=' before an object value
                        nextValue = parseValue(afterKey);
                    } else {
                        if (!isKeyValueSeparatorToken(afterKey)) {
                            throw parseError(addQuoteSuggestion(afterKey.toString(),
                                    "Key '" + path.render() + "' may not be followed by token: "
                                            + afterKey));
                        }

                        keyValueNodes.add(new ConfigNodeSingleToken(afterKey));

                        if (afterKey == Tokens.EQUALS) {
                            insideEquals = true;
                            equalsCount += 1;
                        }

                        nextValue = consolidateValues(keyValueNodes);
                        if (nextValue == null) {
                            nextValue = parseValue(nextTokenCollectingWhitespace(keyValueNodes));
                        }
                    }

                    keyValueNodes.add(nextValue);
                    if (insideEquals) {
                        equalsCount -= 1;
                    }
                    lastInsideEquals = insideEquals;

                    String key = path.value().first();
                    Path remaining = path.value().remainder();

                    if (remaining == null) {
                        Boolean existing = keys.get(key);
                        if (existing != null) {
                            // In strict JSON, dups should be an error; while in
                            // our custom config language, they should be merged
                            // if the value is an object (or substitution that
                            // could become an object).

                            if (flavor == ConfigSyntax.JSON) {
                                throw parseError("JSON does not allow duplicate fields: '"
                                        + key
                                        + "' was already seen");
                            }
                        }
                        keys.put(key, true);
                    } else {
                        if (flavor == ConfigSyntax.JSON) {
                            throw new ConfigException.BugOrBroken(
                                    "somehow got multi-element path in JSON mode");
                        }
                        keys.put(key, true);
                    }

                    afterComma = false;
                    objectNodes.add(new ConfigNodeField(keyValueNodes));
                }

                if (checkElementSeparator(objectNodes)) {
                    // continue looping
                    afterComma = true;
                } else {
                    t = nextTokenCollectingWhitespace(objectNodes);
                    if (t == Tokens.CLOSE_CURLY) {
                        if (!hadOpenCurly) {
                            throw parseError(addQuoteSuggestion(lastPath, lastInsideEquals,
                                    t.toString(), "unbalanced close brace '}' with no open brace"));
                        }
                        objectNodes.add(new ConfigNodeSingleToken(t));
                        break;
                    } else if (hadOpenCurly) {
                        throw parseError(addQuoteSuggestion(lastPath, lastInsideEquals,
                                t.toString(), "Expecting close brace } or a comma, got " + t));
                    } else {
                        if (t == Tokens.END) {
                            putBack(t);
                            break;
                        } else {
                            throw parseError(addQuoteSuggestion(lastPath, lastInsideEquals,
                                    t.toString(), "Expecting end of input or a comma, got " + t));
                        }
                    }
                }
            }

            return new ConfigNodeObject(objectNodes);
        }

        private ConfigNodeComplexValue parseArray() {
            ArrayList<AbstractConfigNode> children = new ArrayList<AbstractConfigNode>();
            children.add(new ConfigNodeSingleToken(Tokens.OPEN_SQUARE));
            // invoked just after the OPEN_SQUARE
            Token t;

            AbstractConfigNodeValue nextValue = consolidateValues(children);
            if (nextValue != null) {
                children.add(nextValue);
            } else {
                t = nextTokenCollectingWhitespace(children);

                // special-case the first element
                if (t == Tokens.CLOSE_SQUARE) {
                    children.add(new ConfigNodeSingleToken(t));
                    return new ConfigNodeArray(children);
                } else if (Tokens.isValue(t) || t == Tokens.OPEN_CURLY
                        || t == Tokens.OPEN_SQUARE || Tokens.isUnquotedText(t)
                        || Tokens.isSubstitution(t)) {
                    nextValue = parseValue(t);
                    children.add(nextValue);
                } else {
                    throw parseError("List should have ] or a first element after the open [, instead had token: "
                            + t
                            + " (if you want "
                            + t
                            + " to be part of a string value, then double-quote it)");
                }
            }

            // now remaining elements
            while (true) {
                // just after a value
                if (checkElementSeparator(children)) {
                    // comma (or newline equivalent) consumed
                } else {
                    t = nextTokenCollectingWhitespace(children);
                    if (t == Tokens.CLOSE_SQUARE) {
                        children.add(new ConfigNodeSingleToken(t));
                        return new ConfigNodeArray(children);
                    } else {
                        throw parseError("List should have ended with ] or had a comma, instead had token: "
                                + t
                                + " (if you want "
                                + t
                                + " to be part of a string value, then double-quote it)");
                    }
                }

                // now just after a comma
                nextValue = consolidateValues(children);
                if (nextValue != null) {
                    children.add(nextValue);
                } else {
                    t = nextTokenCollectingWhitespace(children);
                    if (Tokens.isValue(t) || t == Tokens.OPEN_CURLY
                            || t == Tokens.OPEN_SQUARE || Tokens.isUnquotedText(t)
                            || Tokens.isSubstitution(t)) {
                        nextValue = parseValue(t);
                        children.add(nextValue);
                    } else if (flavor != ConfigSyntax.JSON && t == Tokens.CLOSE_SQUARE) {
                        // we allow one trailing comma
                        putBack(t);
                    } else {
                        throw parseError("List should have had new element after a comma, instead had token: "
                                + t
                                + " (if you want the comma or "
                                + t
                                + " to be part of a string value, then double-quote it)");
                    }
                }
            }
        }

        ConfigNodeRoot parse() {
            ArrayList<AbstractConfigNode> children = new ArrayList<AbstractConfigNode>();
            Token t = nextToken();
            if (t == Tokens.START) {
                // OK
            } else {
                throw new ConfigException.BugOrBroken(
                        "token stream did not begin with START, had " + t);
            }

            t = nextTokenCollectingWhitespace(children);
            AbstractConfigNode result = null;
            boolean missingCurly = false;
            if (t == Tokens.OPEN_CURLY || t == Tokens.OPEN_SQUARE) {
                result = parseValue(t);
            } else {
                if (flavor == ConfigSyntax.JSON) {
                    if (t == Tokens.END) {
                        throw parseError("Empty document");
                    } else {
                        throw parseError("Document must have an object or array at root, unexpected token: "
                                + t);
                    }
                } else {
                    // the root object can omit the surrounding braces.
                    // this token should be the first field's key, or part
                    // of it, so put it back.
                    putBack(t);
                    missingCurly = true;
                    result = parseObject(false);
                }
            }
            // Need to pull the children out of the resulting node so we can keep leading
            // and trailing whitespace if this was a no-brace object. Otherwise, we need to add
            // the result into the list of children.
            if (result instanceof ConfigNodeObject && missingCurly) {
                children.addAll(((ConfigNodeComplexValue) result).children());
            } else {
                children.add(result);
            }
            t = nextTokenCollectingWhitespace(children);
            if (t == Tokens.END) {
                if (missingCurly) {
                    // If there were no braces, the entire document should be treated as a single object
                    return new ConfigNodeRoot(Collections.singletonList((AbstractConfigNode)new ConfigNodeObject(children)), baseOrigin);
                } else {
                    return new ConfigNodeRoot(children, baseOrigin);
                }
            } else {
                throw parseError("Document has trailing tokens after first object or array: "
                        + t);
            }
        }

        // Parse a given input stream into a single value node. Used when doing a replace inside a ConfigDocument.
        AbstractConfigNodeValue parseSingleValue() {
            Token t = nextToken();
            if (t == Tokens.START) {
                // OK
            } else {
                throw new ConfigException.BugOrBroken(
                        "token stream did not begin with START, had " + t);
            }

            t = nextToken();
            if (Tokens.isIgnoredWhitespace(t) || Tokens.isNewline(t) || isUnquotedWhitespace(t) || Tokens.isComment(t)) {
                throw parseError("The value from withValueText cannot have leading or trailing newlines, whitespace, or comments");
            }
            if (t == Tokens.END) {
                throw parseError("Empty value");
            }
            if (flavor == ConfigSyntax.JSON) {
                AbstractConfigNodeValue node = parseValue(t);
                t = nextToken();
                if (t == Tokens.END) {
                    return node;
                } else {
                    throw parseError("Parsing JSON and the value set in withValueText was either a concatenation or " +
                                        "had trailing whitespace, newlines, or comments");
                }
            } else {
                putBack(t);
                ArrayList<AbstractConfigNode> nodes = new ArrayList<AbstractConfigNode>();
                AbstractConfigNodeValue node = consolidateValues(nodes);
                t = nextToken();
                if (t == Tokens.END) {
                    return node;
                } else {
                    throw parseError("The value from withValueText cannot have leading or trailing newlines, whitespace, or comments");
                }
            }
        }
    }
}
