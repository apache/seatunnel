/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigException;
import io.github.interestinglab.waterdrop.config.ConfigOrigin;
import io.github.interestinglab.waterdrop.config.ConfigSyntax;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

final class Tokenizer {
    // this exception should not leave this file
    private static class ProblemException extends Exception {
        private static final long serialVersionUID = 1L;

        final private Token problem;

        ProblemException(Token problem) {
            this.problem = problem;
        }

        Token problem() {
            return problem;
        }
    }

    private static String asString(int codepoint) {
        if (codepoint == '\n')
            return "newline";
        else if (codepoint == '\t')
            return "tab";
        else if (codepoint == -1)
            return "end of file";
        else if (ConfigImplUtil.isC0Control(codepoint))
            return String.format("control character 0x%x", codepoint);
        else
            return String.format("%c", codepoint);
    }

    /**
     * Tokenizes a Reader. Does not close the reader; you have to arrange to do
     * that after you're done with the returned iterator.
     */
    static Iterator<Token> tokenize(ConfigOrigin origin, Reader input, ConfigSyntax flavor) {
        return new TokenIterator(origin, input, flavor != ConfigSyntax.JSON);
    }

    static String render(Iterator<Token> tokens) {
        StringBuilder renderedText = new StringBuilder();
        while (tokens.hasNext()) {
            renderedText.append(tokens.next().tokenText());
        }
        return renderedText.toString();
    }

    private static class TokenIterator implements Iterator<Token> {

        private static class WhitespaceSaver {
            // has to be saved inside value concatenations
            private StringBuilder whitespace;
            // may need to value-concat with next value
            private boolean lastTokenWasSimpleValue;

            WhitespaceSaver() {
                whitespace = new StringBuilder();
                lastTokenWasSimpleValue = false;
            }

            void add(int c) {
                whitespace.appendCodePoint(c);
            }

            Token check(Token t, ConfigOrigin baseOrigin, int lineNumber) {
                if (isSimpleValue(t)) {
                    return nextIsASimpleValue(baseOrigin, lineNumber);
                } else {
                    return nextIsNotASimpleValue(baseOrigin, lineNumber);
                }
            }

            // called if the next token is not a simple value;
            // discards any whitespace we were saving between
            // simple values.
            private Token nextIsNotASimpleValue(ConfigOrigin baseOrigin, int lineNumber) {
                lastTokenWasSimpleValue = false;
                return createWhitespaceTokenFromSaver(baseOrigin, lineNumber);
            }

            // called if the next token IS a simple value,
            // so creates a whitespace token if the previous
            // token also was.
            private Token nextIsASimpleValue(ConfigOrigin baseOrigin,
                    int lineNumber) {
                Token t = createWhitespaceTokenFromSaver(baseOrigin, lineNumber);
                if (!lastTokenWasSimpleValue) {
                    lastTokenWasSimpleValue = true;
                }
                return t;
            }

            private Token createWhitespaceTokenFromSaver(ConfigOrigin baseOrigin,
                                                         int lineNumber) {
                if (whitespace.length() > 0) {
                    Token t;
                    if (lastTokenWasSimpleValue) {
                        t = Tokens.newUnquotedText(
                            lineOrigin(baseOrigin, lineNumber),
                            whitespace.toString());
                    } else {
                        t = Tokens.newIgnoredWhitespace(lineOrigin(baseOrigin, lineNumber),
                                                        whitespace.toString());
                    }
                    whitespace.setLength(0); // reset
                    return t;
                }
                return null;
            }
        }

        final private SimpleConfigOrigin origin;
        final private Reader input;
        final private LinkedList<Integer> buffer;
        private int lineNumber;
        private ConfigOrigin lineOrigin;
        final private Queue<Token> tokens;
        final private WhitespaceSaver whitespaceSaver;
        final private boolean allowComments;

        TokenIterator(ConfigOrigin origin, Reader input, boolean allowComments) {
            this.origin = (SimpleConfigOrigin) origin;
            this.input = input;
            this.allowComments = allowComments;
            this.buffer = new LinkedList<Integer>();
            lineNumber = 1;
            lineOrigin = this.origin.withLineNumber(lineNumber);
            tokens = new LinkedList<Token>();
            tokens.add(Tokens.START);
            whitespaceSaver = new WhitespaceSaver();
        }


        // this should ONLY be called from nextCharSkippingComments
        // or when inside a quoted string, or when parsing a sequence
        // like ${ or +=, everything else should use
        // nextCharSkippingComments().
        private int nextCharRaw() {
            if (buffer.isEmpty()) {
                try {
                    return input.read();
                } catch (IOException e) {
                    throw new ConfigException.IO(origin, "read error: "
                            + e.getMessage(), e);
                }
            } else {
                int c = buffer.pop();
                return c;
            }
        }

        private void putBack(int c) {
            if (buffer.size() > 2) {
                throw new ConfigException.BugOrBroken(
                        "bug: putBack() three times, undesirable look-ahead");
            }
            buffer.push(c);
        }

        static boolean isWhitespace(int c) {
            return ConfigImplUtil.isWhitespace(c);
        }

        static boolean isWhitespaceNotNewline(int c) {
            return c != '\n' && ConfigImplUtil.isWhitespace(c);
        }

        private boolean startOfComment(int c) {
            if (c == -1) {
                return false;
            } else {
                if (allowComments) {
                    if (c == '#') {
                        return true;
                    } else if (c == '/') {
                        int maybeSecondSlash = nextCharRaw();
                        // we want to predictably NOT consume any chars
                        putBack(maybeSecondSlash);
                        if (maybeSecondSlash == '/') {
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }

        // get next char, skipping non-newline whitespace
        private int nextCharAfterWhitespace(WhitespaceSaver saver) {
            for (;;) {
                int c = nextCharRaw();

                if (c == -1) {
                    return -1;
                } else {
                    if (isWhitespaceNotNewline(c)) {
                        saver.add(c);
                        continue;
                    } else {
                        return c;
                    }
                }
            }
        }

        private ProblemException problem(String message) {
            return problem("", message, null);
        }

        private ProblemException problem(String what, String message) {
            return problem(what, message, null);
        }

        private ProblemException problem(String what, String message, boolean suggestQuotes) {
            return problem(what, message, suggestQuotes, null);
        }

        private ProblemException problem(String what, String message, Throwable cause) {
            return problem(lineOrigin, what, message, cause);
        }

        private ProblemException problem(String what, String message, boolean suggestQuotes,
                                         Throwable cause) {
            return problem(lineOrigin, what, message, suggestQuotes, cause);
        }

        private static ProblemException problem(ConfigOrigin origin, String what,
                String message,
                Throwable cause) {
            return problem(origin, what, message, false, cause);
        }

        private static ProblemException problem(ConfigOrigin origin, String what, String message,
                                                boolean suggestQuotes, Throwable cause) {
            if (what == null || message == null)
                throw new ConfigException.BugOrBroken(
                        "internal error, creating bad ProblemException");
            return new ProblemException(Tokens.newProblem(origin, what, message, suggestQuotes,
                    cause));
        }

        private static ProblemException problem(ConfigOrigin origin, String message) {
            return problem(origin, "", message, null);
        }

        private static ConfigOrigin lineOrigin(ConfigOrigin baseOrigin,
                int lineNumber) {
            return ((SimpleConfigOrigin) baseOrigin).withLineNumber(lineNumber);
        }

        // ONE char has always been consumed, either the # or the first /, but
        // not both slashes
        private Token pullComment(int firstChar) {
            boolean doubleSlash = false;
            if (firstChar == '/') {
                int discard = nextCharRaw();
                if (discard != '/')
                    throw new ConfigException.BugOrBroken("called pullComment but // not seen");
                doubleSlash = true;
            }

            StringBuilder sb = new StringBuilder();
            for (;;) {
                int c = nextCharRaw();
                if (c == -1 || c == '\n') {
                    putBack(c);
                    if (doubleSlash)
                        return Tokens.newCommentDoubleSlash(lineOrigin, sb.toString());
                    else
                        return Tokens.newCommentHash(lineOrigin, sb.toString());
                } else {
                    sb.appendCodePoint(c);
                }
            }
        }

        // chars JSON allows a number to start with
        static final String firstNumberChars = "0123456789-";
        // chars JSON allows to be part of a number
        static final String numberChars = "0123456789eE+-.";
        // chars that stop an unquoted string
        static final String notInUnquotedText = "$\"{}[]:=,+#`^?!@*&\\";

        // The rules here are intended to maximize convenience while
        // avoiding confusion with real valid JSON. Basically anything
        // that parses as JSON is treated the JSON way and otherwise
        // we assume it's a string and let the parser sort it out.
        private Token pullUnquotedText() {
            ConfigOrigin origin = lineOrigin;
            StringBuilder sb = new StringBuilder();
            int c = nextCharRaw();
            while (true) {
                if (c == -1) {
                    break;
                } else if (notInUnquotedText.indexOf(c) >= 0) {
                    break;
                } else if (isWhitespace(c)) {
                    break;
                } else if (startOfComment(c)) {
                    break;
                } else {
                    sb.appendCodePoint(c);
                }

                // we parse true/false/null tokens as such no matter
                // what is after them, as long as they are at the
                // start of the unquoted token.
                if (sb.length() == 4) {
                    String s = sb.toString();
                    if (s.equals("true"))
                        return Tokens.newBoolean(origin, true);
                    else if (s.equals("null"))
                        return Tokens.newNull(origin);
                } else if (sb.length() == 5) {
                    String s = sb.toString();
                    if (s.equals("false"))
                        return Tokens.newBoolean(origin, false);
                }

                c = nextCharRaw();
            }

            // put back the char that ended the unquoted text
            putBack(c);

            String s = sb.toString();
            return Tokens.newUnquotedText(origin, s);
        }

        private Token pullNumber(int firstChar) throws ProblemException {
            StringBuilder sb = new StringBuilder();
            sb.appendCodePoint(firstChar);
            boolean containedDecimalOrE = false;
            int c = nextCharRaw();
            while (c != -1 && numberChars.indexOf(c) >= 0) {
                if (c == '.' || c == 'e' || c == 'E')
                    containedDecimalOrE = true;
                sb.appendCodePoint(c);
                c = nextCharRaw();
            }
            // the last character we looked at wasn't part of the number, put it
            // back
            putBack(c);
            String s = sb.toString();
            try {
                if (containedDecimalOrE) {
                    // force floating point representation
                    return Tokens.newDouble(lineOrigin, Double.parseDouble(s), s);
                } else {
                    // this should throw if the integer is too large for Long
                    return Tokens.newLong(lineOrigin, Long.parseLong(s), s);
                }
            } catch (NumberFormatException e) {
                // not a number after all, see if it's an unquoted string.
                for (char u : s.toCharArray()) {
                    if (notInUnquotedText.indexOf(u) >= 0)
                        throw problem(asString(u), "Reserved character '" + asString(u)
                                      + "' is not allowed outside quotes", true /* suggestQuotes */);
                }
                // no evil chars so we just decide this was a string and
                // not a number.
                return Tokens.newUnquotedText(lineOrigin, s);
            }
        }

        private void pullEscapeSequence(StringBuilder sb, StringBuilder sbOrig) throws ProblemException {
            int escaped = nextCharRaw();
            if (escaped == -1)
                throw problem("End of input but backslash in string had nothing after it");

            // This is needed so we return the unescaped escape characters back out when rendering
            // the token
            sbOrig.appendCodePoint('\\');
            sbOrig.appendCodePoint(escaped);

            switch (escaped) {
            case '"':
                sb.append('"');
                break;
            case '\\':
                sb.append('\\');
                break;
            case '/':
                sb.append('/');
                break;
            case 'b':
                sb.append('\b');
                break;
            case 'f':
                sb.append('\f');
                break;
            case 'n':
                sb.append('\n');
                break;
            case 'r':
                sb.append('\r');
                break;
            case 't':
                sb.append('\t');
                break;
            case 'u': {
                // kind of absurdly slow, but screw it for now
                char[] a = new char[4];
                for (int i = 0; i < 4; ++i) {
                    int c = nextCharRaw();
                    if (c == -1)
                        throw problem("End of input but expecting 4 hex digits for \\uXXXX escape");
                    a[i] = (char) c;
                }
                String digits = new String(a);
                sbOrig.append(a);
                try {
                    sb.appendCodePoint(Integer.parseInt(digits, 16));
                } catch (NumberFormatException e) {
                    throw problem(digits, String.format(
                            "Malformed hex digits after \\u escape in string: '%s'", digits), e);
                }
            }
                break;
            default:
                throw problem(
                        asString(escaped),
                        String.format(
                                "backslash followed by '%s', this is not a valid escape sequence (quoted strings use JSON escaping, so use double-backslash \\\\ for literal backslash)",
                                asString(escaped)));
            }
        }

        private void appendTripleQuotedString(StringBuilder sb, StringBuilder sbOrig) throws ProblemException {
            // we are after the opening triple quote and need to consume the
            // close triple
            int consecutiveQuotes = 0;
            for (;;) {
                int c = nextCharRaw();

                if (c == '"') {
                    consecutiveQuotes += 1;
                } else if (consecutiveQuotes >= 3) {
                    // the last three quotes end the string and the others are
                    // kept.
                    sb.setLength(sb.length() - 3);
                    putBack(c);
                    break;
                } else {
                    consecutiveQuotes = 0;
                    if (c == -1)
                        throw problem("End of input but triple-quoted string was still open");
                    else if (c == '\n') {
                        // keep the line number accurate
                        lineNumber += 1;
                        lineOrigin = origin.withLineNumber(lineNumber);
                    }
                }

                sb.appendCodePoint(c);
                sbOrig.appendCodePoint(c);
            }
        }

        private Token pullQuotedString() throws ProblemException {
            // the open quote has already been consumed
            StringBuilder sb = new StringBuilder();

            // We need a second string builder to keep track of escape characters.
            // We want to return them exactly as they appeared in the original text,
            // which means we will need a new StringBuilder to escape escape characters
            // so we can also keep the actual value of the string. This is gross.
            StringBuilder sbOrig = new StringBuilder();
            sbOrig.appendCodePoint('"');

            while (true) {
                int c = nextCharRaw();
                if (c == -1)
                    throw problem("End of input but string quote was still open");

                if (c == '\\') {
                    pullEscapeSequence(sb, sbOrig);
                } else if (c == '"') {
                    sbOrig.appendCodePoint(c);
                    break;
                } else if (ConfigImplUtil.isC0Control(c)) {
                    throw problem(asString(c), "JSON does not allow unescaped " + asString(c)
                            + " in quoted strings, use a backslash escape");
                } else {
                    sb.appendCodePoint(c);
                    sbOrig.appendCodePoint(c);
                }
            }

            // maybe switch to triple-quoted string, sort of hacky...
            if (sb.length() == 0) {
                int third = nextCharRaw();
                if (third == '"') {
                    sbOrig.appendCodePoint(third);
                    appendTripleQuotedString(sb, sbOrig);
                } else {
                    putBack(third);
                }

            }
            return Tokens.newString(lineOrigin, sb.toString(), sbOrig.toString());
        }

        private Token pullPlusEquals() throws ProblemException {
            // the initial '+' has already been consumed
            int c = nextCharRaw();
            if (c != '=') {
                throw problem(asString(c), "'+' not followed by =, '" + asString(c)
                        + "' not allowed after '+'", true /* suggestQuotes */);
            }
            return Tokens.PLUS_EQUALS;
        }

        private Token pullSubstitution() throws ProblemException {
            // the initial '$' has already been consumed
            ConfigOrigin origin = lineOrigin;
            int c = nextCharRaw();
            if (c != '{') {
                throw problem(asString(c), "'$' not followed by {, '" + asString(c)
                        + "' not allowed after '$'", true /* suggestQuotes */);
            }

            boolean optional = false;
            c = nextCharRaw();
            if (c == '?') {
                optional = true;
            } else {
                putBack(c);
            }

            WhitespaceSaver saver = new WhitespaceSaver();
            List<Token> expression = new ArrayList<Token>();

            Token t;
            do {
                t = pullNextToken(saver);

                // note that we avoid validating the allowed tokens inside
                // the substitution here; we even allow nested substitutions
                // in the tokenizer. The parser sorts it out.
                if (t == Tokens.CLOSE_CURLY) {
                    // end the loop, done!
                    break;
                } else if (t == Tokens.END) {
                    throw problem(origin,
                            "Substitution ${ was not closed with a }");
                } else {
                    Token whitespace = saver.check(t, origin, lineNumber);
                    if (whitespace != null)
                        expression.add(whitespace);
                    expression.add(t);
                }
            } while (true);

            return Tokens.newSubstitution(origin, optional, expression);
        }

        private Token pullNextToken(WhitespaceSaver saver) throws ProblemException {
            int c = nextCharAfterWhitespace(saver);
            if (c == -1) {
                return Tokens.END;
            } else if (c == '\n') {
                // newline tokens have the just-ended line number
                Token line = Tokens.newLine(lineOrigin);
                lineNumber += 1;
                lineOrigin = origin.withLineNumber(lineNumber);
                return line;
            } else {
                Token t;
                if (startOfComment(c)) {
                    t = pullComment(c);
                } else {
                    switch (c) {
                    case '"':
                        t = pullQuotedString();
                        break;
                    case '$':
                        t = pullSubstitution();
                        break;
                    case ':':
                        t = Tokens.COLON;
                        break;
                    case ',':
                        t = Tokens.COMMA;
                        break;
                    case '=':
                        t = Tokens.EQUALS;
                        break;
                    case '{':
                        t = Tokens.OPEN_CURLY;
                        break;
                    case '}':
                        t = Tokens.CLOSE_CURLY;
                        break;
                    case '[':
                        t = Tokens.OPEN_SQUARE;
                        break;
                    case ']':
                        t = Tokens.CLOSE_SQUARE;
                        break;
                    case '+':
                        t = pullPlusEquals();
                        break;
                    default:
                        t = null;
                        break;
                    }

                    if (t == null) {
                        if (firstNumberChars.indexOf(c) >= 0) {
                            t = pullNumber(c);
                        } else if (notInUnquotedText.indexOf(c) >= 0) {
                            throw problem(asString(c), "Reserved character '" + asString(c)
                                    + "' is not allowed outside quotes", true /* suggestQuotes */);
                        } else {
                            putBack(c);
                            t = pullUnquotedText();
                        }
                    }
                }

                if (t == null)
                    throw new ConfigException.BugOrBroken(
                            "bug: failed to generate next token");

                return t;
            }
        }

        private static boolean isSimpleValue(Token t) {
            if (Tokens.isSubstitution(t) || Tokens.isUnquotedText(t)
                    || Tokens.isValue(t)) {
                return true;
            } else {
                return false;
            }
        }

        private void queueNextToken() throws ProblemException {
            Token t = pullNextToken(whitespaceSaver);
            Token whitespace = whitespaceSaver.check(t, origin, lineNumber);
            if (whitespace != null)
                tokens.add(whitespace);

            tokens.add(t);
        }

        @Override
        public boolean hasNext() {
            return !tokens.isEmpty();
        }

        @Override
        public Token next() {
            Token t = tokens.remove();
            if (tokens.isEmpty() && t != Tokens.END) {
                try {
                    queueNextToken();
                } catch (ProblemException e) {
                    tokens.add(e.problem());
                }
                if (tokens.isEmpty())
                    throw new ConfigException.BugOrBroken(
                            "bug: tokens queue should not be empty here");
            }
            return t;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException(
                    "Does not make sense to remove items from token stream");
        }
    }
}
