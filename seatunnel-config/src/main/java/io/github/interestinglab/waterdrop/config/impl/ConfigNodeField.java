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

package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

final class ConfigNodeField extends AbstractConfigNode {
    private final ArrayList<AbstractConfigNode> children;

    public ConfigNodeField(Collection<AbstractConfigNode> children) {
        this.children = new ArrayList<AbstractConfigNode>(children);
    }

    @Override
    protected Collection<Token> tokens() {
        ArrayList<Token> tokens = new ArrayList<Token>();
        for (AbstractConfigNode child : children) {
            tokens.addAll(child.tokens());
        }
        return tokens;
    }

    public ConfigNodeField replaceValue(AbstractConfigNodeValue newValue) {
        ArrayList<AbstractConfigNode> childrenCopy = new ArrayList<AbstractConfigNode>(children);
        for (int i = 0; i < childrenCopy.size(); i++) {
            if (childrenCopy.get(i) instanceof AbstractConfigNodeValue) {
                childrenCopy.set(i, newValue);
                return new ConfigNodeField(childrenCopy);
            }
        }
        throw new ConfigException.BugOrBroken("Field node doesn't have a value");
    }

    public AbstractConfigNodeValue value() {
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i) instanceof AbstractConfigNodeValue) {
                return (AbstractConfigNodeValue) children.get(i);
            }
        }
        throw new ConfigException.BugOrBroken("Field node doesn't have a value");
    }

    public ConfigNodePath path() {
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i) instanceof ConfigNodePath) {
                return (ConfigNodePath) children.get(i);
            }
        }
        throw new ConfigException.BugOrBroken("Field node doesn't have a path");
    }

    protected Token separator() {
        for (AbstractConfigNode child : children) {
            if (child instanceof ConfigNodeSingleToken) {
                Token t = ((ConfigNodeSingleToken) child).token();
                if (t == Tokens.PLUS_EQUALS || t == Tokens.COLON || t == Tokens.EQUALS) {
                    return t;
                }
            }
        }
        return null;
    }

    protected List<String> comments() {
        List<String> comments = new ArrayList<String>();
        for (AbstractConfigNode child : children) {
            if (child instanceof ConfigNodeComment) {
                comments.add(((ConfigNodeComment) child).commentText());
            }
        }
        return comments;
    }
}
