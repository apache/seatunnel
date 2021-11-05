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
import io.github.interestinglab.waterdrop.config.ConfigParseOptions;
import io.github.interestinglab.waterdrop.config.ConfigRenderOptions;
import io.github.interestinglab.waterdrop.config.ConfigValue;
import io.github.interestinglab.waterdrop.config.parser.ConfigDocument;

import java.io.StringReader;
import java.util.Iterator;

final class SimpleConfigDocument implements ConfigDocument {
    private ConfigNodeRoot configNodeTree;
    private ConfigParseOptions parseOptions;

    SimpleConfigDocument(ConfigNodeRoot parsedNode, ConfigParseOptions parseOptions) {
        configNodeTree = parsedNode;
        this.parseOptions = parseOptions;
    }

    @Override
    public ConfigDocument withValueText(String path, String newValue) {
        if (newValue == null)
            throw new ConfigException.BugOrBroken("null value for " + path + " passed to withValueText");
        SimpleConfigOrigin origin = SimpleConfigOrigin.newSimple("single value parsing");
        StringReader reader = new StringReader(newValue);
        Iterator<Token> tokens = Tokenizer.tokenize(origin, reader, parseOptions.getSyntax());
        AbstractConfigNodeValue parsedValue = ConfigDocumentParser.parseValue(tokens, origin, parseOptions);
        reader.close();

        return new SimpleConfigDocument(configNodeTree.setValue(path, parsedValue, parseOptions.getSyntax()), parseOptions);
    }

    @Override
    public ConfigDocument withValue(String path, ConfigValue newValue) {
        if (newValue == null)
            throw new ConfigException.BugOrBroken("null value for " + path + " passed to withValue");
        ConfigRenderOptions options = ConfigRenderOptions.defaults();
        options = options.setOriginComments(false);
        return withValueText(path, newValue.render(options).trim());
    }

    @Override
    public ConfigDocument withoutPath(String path) {
        return new SimpleConfigDocument(configNodeTree.setValue(path, null, parseOptions.getSyntax()), parseOptions);
    }

    @Override
    public boolean hasPath(String path) {
        return configNodeTree.hasValue(path);
    }

    public String render() {
        return configNodeTree.render();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ConfigDocument && render().equals(((ConfigDocument) other).render());
    }

    @Override
    public int hashCode() {
        return render().hashCode();
    }
}
