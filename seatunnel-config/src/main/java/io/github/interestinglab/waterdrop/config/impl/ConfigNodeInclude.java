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

import java.util.ArrayList;
import java.util.Collection;

final class ConfigNodeInclude extends AbstractConfigNode {
    final private ArrayList<AbstractConfigNode> children;
    final private ConfigIncludeKind kind;
    final private boolean isRequired;

    ConfigNodeInclude(Collection<AbstractConfigNode> children, ConfigIncludeKind kind, boolean isRequired) {
        this.children = new ArrayList<AbstractConfigNode>(children);
        this.kind = kind;
        this.isRequired = isRequired;
    }

    final public Collection<AbstractConfigNode> children() {
        return children;
    }

    @Override
    protected Collection<Token> tokens() {
        ArrayList<Token> tokens = new ArrayList<Token>();
        for (AbstractConfigNode child : children) {
            tokens.addAll(child.tokens());
        }
        return tokens;
    }

    protected ConfigIncludeKind kind() {
        return kind;
    }

    protected boolean isRequired() {
        return isRequired;
    }

    protected String name() {
        for (AbstractConfigNode n : children) {
            if (n instanceof ConfigNodeSimpleValue) {
                return (String) Tokens.getValue(((ConfigNodeSimpleValue) n).token()).unwrapped();
            }
        }
        return null;
    }
}
