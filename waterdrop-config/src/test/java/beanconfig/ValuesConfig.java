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
package beanconfig;

import java.util.Map;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.config.ConfigList;
import io.github.interestinglab.waterdrop.config.ConfigObject;
import io.github.interestinglab.waterdrop.config.ConfigValue;

// test bean for various "uncooked" values
public class ValuesConfig {

    Object obj;
    Config config;
    ConfigObject configObj;
    ConfigValue configValue;
    ConfigList list;
    Map<String,Object> unwrappedMap;

    public Object getObj() {
        return obj;
    }

    public void setObj(Object obj) {
        this.obj = obj;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public ConfigObject getConfigObj() {
        return configObj;
    }

    public void setConfigObj(ConfigObject configObj) {
        this.configObj = configObj;
    }

    public ConfigValue getConfigValue() {
        return configValue;
    }

    public void setConfigValue(ConfigValue configValue) {
        this.configValue = configValue;
    }

    public ConfigList getList() {
        return list;
    }

    public void setList(ConfigList list) {
        this.list = list;
    }

    public Map<String, Object> getUnwrappedMap() {
        return unwrappedMap;
    }

    public void setUnwrappedMap(Map<String, Object> unwrappedMap) {
        this.unwrappedMap = unwrappedMap;
    }

}
