/**
 *   Copyright (C) 2015 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigException;
import io.github.interestinglab.waterdrop.config.ConfigParseOptions;

import java.util.ArrayList;
import java.util.Collection;

final class ConfigNodePath extends AbstractConfigNode {
    final private Path path;
    final ArrayList<Token> tokens;
    ConfigNodePath(Path path, Collection<Token> tokens) {
        this.path = path;
        this.tokens = new ArrayList<Token>(tokens);
    }

    @Override
    protected Collection<Token> tokens() {
        return tokens;
    }

    protected Path value() {
        return path;
    }

    protected ConfigNodePath subPath(int toRemove) {
        int periodCount = 0;
        ArrayList<Token> tokensCopy = new ArrayList<Token>(tokens);
        for (int i = 0; i < tokensCopy.size(); i++) {
            if (Tokens.isUnquotedText(tokensCopy.get(i)) &&
                    tokensCopy.get(i).tokenText().equals(ConfigParseOptions.pathTokenSeparator))
                periodCount++;

            if (periodCount == toRemove) {
                return new ConfigNodePath(path.subPath(toRemove), tokensCopy.subList(i + 1, tokensCopy.size()));
            }
        }
        throw new ConfigException.BugOrBroken("Tried to remove too many elements from a Path node");
    }

    protected ConfigNodePath first() {
        ArrayList<Token> tokensCopy = new ArrayList<Token>(tokens);
        for (int i = 0; i < tokensCopy.size(); i++) {
            if (Tokens.isUnquotedText(tokensCopy.get(i)) &&
                    tokensCopy.get(i).tokenText().equals(ConfigParseOptions.pathTokenSeparator))
                return new ConfigNodePath(path.subPath(0, 1), tokensCopy.subList(0, i));
        }
        return this;
    }
}
