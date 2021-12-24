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

    protected Token token() {
        return token;
    }

    protected AbstractConfigValue value() {
        if (Tokens.isValue(token)) {
            return Tokens.getValue(token);
        } else if (Tokens.isUnquotedText(token)) {
            return new ConfigString.Unquoted(token.origin(), Tokens.getUnquotedText(token));
        } else if (Tokens.isSubstitution(token)) {
            List<Token> expression = Tokens.getSubstitutionPathExpression(token);
            Path path = PathParser.parsePathExpression(expression.iterator(), token.origin());
            boolean optional = Tokens.getSubstitutionOptional(token);

            return new ConfigReference(token.origin(), new SubstitutionExpression(path, optional));
        }
        throw new BugOrBroken("ConfigNodeSimpleValue did not contain a valid value token");
    }
}
