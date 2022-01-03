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

package org.apache.seatunnel.config.impl;

import org.apache.seatunnel.config.ConfigException.BugOrBroken;

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
        throw new BugOrBroken("Field node doesn't have a value");
    }

    public AbstractConfigNodeValue value() {
        for (AbstractConfigNode child : children) {
            if (child instanceof AbstractConfigNodeValue) {
                return (AbstractConfigNodeValue) child;
            }
        }
        throw new BugOrBroken("Field node doesn't have a value");
    }

    public ConfigNodePath path() {
        for (AbstractConfigNode child : children) {
            if (child instanceof ConfigNodePath) {
                return (ConfigNodePath) child;
            }
        }
        throw new BugOrBroken("Field node doesn't have a path");
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
