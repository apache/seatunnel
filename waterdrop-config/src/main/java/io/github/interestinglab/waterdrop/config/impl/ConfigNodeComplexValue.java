/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
/**
 *   Copyright (C) 2015 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import java.util.ArrayList;
import java.util.Collection;

abstract class ConfigNodeComplexValue extends AbstractConfigNodeValue {
    final protected ArrayList<AbstractConfigNode> children;

    ConfigNodeComplexValue(Collection<AbstractConfigNode> children) {
        this.children = new ArrayList<AbstractConfigNode>(children);
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

    protected ConfigNodeComplexValue indentText(AbstractConfigNode indentation) {
        ArrayList<AbstractConfigNode> childrenCopy = new ArrayList<AbstractConfigNode>(children);
        for (int i = 0; i < childrenCopy.size(); i++) {
            AbstractConfigNode child = childrenCopy.get(i);
            if (child instanceof ConfigNodeSingleToken &&
                    Tokens.isNewline(((ConfigNodeSingleToken) child).token())) {
                childrenCopy.add(i + 1, indentation);
                i++;
            } else if (child instanceof ConfigNodeField) {
                AbstractConfigNode value = ((ConfigNodeField) child).value();
                if (value instanceof ConfigNodeComplexValue) {
                    childrenCopy.set(i, ((ConfigNodeField) child).replaceValue(((ConfigNodeComplexValue) value).indentText(indentation)));
                }
            } else if (child instanceof ConfigNodeComplexValue) {
                childrenCopy.set(i, ((ConfigNodeComplexValue) child).indentText(indentation));
            }
        }
        return newNode(childrenCopy);
    }

    // This method will just call into the object's constructor, but it's needed
    // for use in the indentText() method so we can avoid a gross if/else statement
    // checking the type of this
    abstract ConfigNodeComplexValue newNode(Collection<AbstractConfigNode> nodes);
}
