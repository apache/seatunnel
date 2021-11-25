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

package beanconfig;

import java.util.List;
import java.time.Duration;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.config.ConfigMemorySize;
import io.github.interestinglab.waterdrop.config.ConfigObject;
import io.github.interestinglab.waterdrop.config.ConfigValue;

public class ArraysConfig {

    List<Integer> empty;
    List<Integer> ofInt;
    List<String> ofString;
    List<Double> ofDouble;
    List<Long> ofLong;
    List<Object> ofNull;
    List<Boolean> ofBoolean;
    List<Object> ofObject;
    List<Config> ofConfig;
    List<ConfigObject> ofConfigObject;
    List<ConfigValue> ofConfigValue;
    List<Duration> ofDuration;
    List<ConfigMemorySize> ofMemorySize;
    List<StringsConfig> ofStringBean;

    public List<Integer> getEmpty() {
        return empty;
    }

    public void setEmpty(List<Integer> empty) {
        this.empty = empty;
    }

    public List<Integer> getOfInt() {
        return ofInt;
    }

    public void setOfInt(List<Integer> ofInt) {
        this.ofInt = ofInt;
    }

    public List<String> getOfString() {
        return ofString;
    }

    public void setOfString(List<String> ofString) {
        this.ofString = ofString;
    }

    public List<Double> getOfDouble() {
        return ofDouble;
    }

    public void setOfDouble(List<Double> ofDouble) {
        this.ofDouble = ofDouble;
    }

    public List<Object> getOfNull() {
        return ofNull;
    }

    public void setOfNull(List<Object> ofNull) {
        this.ofNull = ofNull;
    }

    public List<Boolean> getOfBoolean() {
        return ofBoolean;
    }

    public void setOfBoolean(List<Boolean> ofBoolean) {
        this.ofBoolean = ofBoolean;
    }

    public List<Object> getOfObject() {
        return ofObject;
    }

    public void setOfObject(List<Object> ofObject) {
        this.ofObject = ofObject;
    }

    public List<Long> getOfLong() {
        return ofLong;
    }

    public void setOfLong(List<Long> ofLong) {
        this.ofLong = ofLong;
    }

    public List<Config> getOfConfig() {
        return ofConfig;
    }

    public void setOfConfig(List<Config> ofConfig) {
        this.ofConfig = ofConfig;
    }

    public List<ConfigObject> getOfConfigObject() {
        return ofConfigObject;
    }

    public void setOfConfigObject(List<ConfigObject> ofConfigObject) {
        this.ofConfigObject = ofConfigObject;
    }

    public List<ConfigValue> getOfConfigValue() {
        return ofConfigValue;
    }

    public void setOfConfigValue(List<ConfigValue> ofConfigValue) {
        this.ofConfigValue = ofConfigValue;
    }

    public List<Duration> getOfDuration() {
        return ofDuration;
    }

    public void setOfDuration(List<Duration> ofDuration) {
        this.ofDuration = ofDuration;
    }

    public List<ConfigMemorySize> getOfMemorySize() {
        return ofMemorySize;
    }

    public void setOfMemorySize(List<ConfigMemorySize> ofMemorySize) {
        this.ofMemorySize = ofMemorySize;
    }

    public List<StringsConfig> getOfStringBean() {
        return ofStringBean;
    }

    public void setOfStringBean(List<StringsConfig> ofStringBean) {
        this.ofStringBean = ofStringBean;
    }
}
