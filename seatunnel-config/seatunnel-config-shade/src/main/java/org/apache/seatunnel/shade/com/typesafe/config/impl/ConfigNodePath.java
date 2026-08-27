/*
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */

package org.apache.seatunnel.shade.com.typesafe.config.impl;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;

import java.util.ArrayList;
import java.util.Collection;

final class ConfigNodePath extends AbstractConfigNode {
    private final Path path;
    final ArrayList<Token> tokens;

    ConfigNodePath(Path path, Collection<Token> tokens) {
        this.path = path;
        this.tokens = new ArrayList<>(tokens);
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
        ArrayList<Token> tokensCopy = new ArrayList<>(tokens);
        for (int i = 0; i < tokensCopy.size(); i++) {
            if (Tokens.isUnquotedText(tokensCopy.get(i))
                    && tokensCopy
                            .get(i)
                            .tokenText()
                            .equals(ConfigParseOptions.PATH_TOKEN_SEPARATOR)) {
                periodCount++;
            }

            if (periodCount == toRemove) {
                return new ConfigNodePath(
                        path.subPath(toRemove), tokensCopy.subList(i + 1, tokensCopy.size()));
            }
        }
        throw new ConfigException.BugOrBroken("Tried to remove too many elements from a Path node");
    }

    protected ConfigNodePath first() {
        ArrayList<Token> tokensCopy = new ArrayList<>(tokens);
        for (int i = 0; i < tokensCopy.size(); i++) {
            if (Tokens.isUnquotedText(tokensCopy.get(i))
                    && tokensCopy
                            .get(i)
                            .tokenText()
                            .equals(ConfigParseOptions.PATH_TOKEN_SEPARATOR)) {
                return new ConfigNodePath(path.subPath(0, 1), tokensCopy.subList(0, i));
            }
        }
        return this;
    }
}
