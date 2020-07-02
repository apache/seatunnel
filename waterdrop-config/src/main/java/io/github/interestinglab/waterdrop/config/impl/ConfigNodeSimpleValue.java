/**
 *   Copyright (C) 2015 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

final class ConfigNodeSimpleValue extends AbstractConfigNodeValue {
    final Token token;
    ConfigNodeSimpleValue(Token value) {
        token = value;
    }

    @Override
    protected Collection<Token> tokens() {
        return Collections.singletonList(token);
    }

    protected Token token() { return token; }

    protected AbstractConfigValue value() {
        if (Tokens.isValue(token))
            return Tokens.getValue(token);
        else if (Tokens.isUnquotedText(token))
            return new ConfigString.Unquoted(token.origin(), Tokens.getUnquotedText(token));
        else if (Tokens.isSubstitution(token)) {
            List<Token> expression = Tokens.getSubstitutionPathExpression(token);
            Path path = PathParser.parsePathExpression(expression.iterator(), token.origin());
            boolean optional = Tokens.getSubstitutionOptional(token);

            return new ConfigReference(token.origin(), new SubstitutionExpression(path, optional));
        }
        throw new ConfigException.BugOrBroken("ConfigNodeSimpleValue did not contain a valid value token");
    }
}
