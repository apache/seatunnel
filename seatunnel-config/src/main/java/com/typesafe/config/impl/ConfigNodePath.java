/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.typesafe.config.impl;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigParseOptions;

import java.util.ArrayList;
import java.util.Collection;

final class ConfigNodePath extends AbstractConfigNode {
    private final Path path;
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
                tokensCopy.get(i).tokenText().equals(ConfigParseOptions.PATH_TOKEN_SEPARATOR)) {
                periodCount++;
            }

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
                tokensCopy.get(i).tokenText().equals(ConfigParseOptions.PATH_TOKEN_SEPARATOR)) {
                return new ConfigNodePath(path.subPath(0, 1), tokensCopy.subList(0, i));
            }
        }
        return this;
    }
}
