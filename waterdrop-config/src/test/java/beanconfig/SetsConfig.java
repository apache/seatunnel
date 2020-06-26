package beanconfig;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.config.ConfigMemorySize;
import io.github.interestinglab.waterdrop.config.ConfigObject;
import io.github.interestinglab.waterdrop.config.ConfigValue;

import java.time.Duration;
import java.util.Set;

public class SetsConfig {

    Set<Integer> empty;
    Set<Integer> ofInt;
    Set<String> ofString;
    Set<Double> ofDouble;
    Set<Long> ofLong;
    Set<Object> ofNull;
    Set<Boolean> ofBoolean;
    Set<Object> ofObject;
    Set<Config> ofConfig;
    Set<ConfigObject> ofConfigObject;
    Set<ConfigValue> ofConfigValue;
    Set<Duration> ofDuration;
    Set<ConfigMemorySize> ofMemorySize;
    Set<StringsConfig> ofStringBean;

    public Set<Integer> getEmpty() {
        return empty;
    }

    public void setEmpty(Set<Integer> empty) {
        this.empty = empty;
    }

    public Set<Integer> getOfInt() {
        return ofInt;
    }

    public void setOfInt(Set<Integer> ofInt) {
        this.ofInt = ofInt;
    }

    public Set<String> getOfString() {
        return ofString;
    }

    public void setOfString(Set<String> ofString) {
        this.ofString = ofString;
    }

    public Set<Double> getOfDouble() {
        return ofDouble;
    }

    public void setOfDouble(Set<Double> ofDouble) {
        this.ofDouble = ofDouble;
    }

    public Set<Object> getOfNull() {
        return ofNull;
    }

    public void setOfNull(Set<Object> ofNull) {
        this.ofNull = ofNull;
    }

    public Set<Boolean> getOfBoolean() {
        return ofBoolean;
    }

    public void setOfBoolean(Set<Boolean> ofBoolean) {
        this.ofBoolean = ofBoolean;
    }

    public Set<Object> getOfObject() {
        return ofObject;
    }

    public void setOfObject(Set<Object> ofObject) {
        this.ofObject = ofObject;
    }

    public Set<Long> getOfLong() {
        return ofLong;
    }

    public void setOfLong(Set<Long> ofLong) {
        this.ofLong = ofLong;
    }

    public Set<Config> getOfConfig() {
        return ofConfig;
    }

    public void setOfConfig(Set<Config> ofConfig) {
        this.ofConfig = ofConfig;
    }

    public Set<ConfigObject> getOfConfigObject() {
        return ofConfigObject;
    }

    public void setOfConfigObject(Set<ConfigObject> ofConfigObject) {
        this.ofConfigObject = ofConfigObject;
    }

    public Set<ConfigValue> getOfConfigValue() {
        return ofConfigValue;
    }

    public void setOfConfigValue(Set<ConfigValue> ofConfigValue) {
        this.ofConfigValue = ofConfigValue;
    }

    public Set<Duration> getOfDuration() {
        return ofDuration;
    }

    public void setOfDuration(Set<Duration> ofDuration) {
        this.ofDuration = ofDuration;
    }

    public Set<ConfigMemorySize> getOfMemorySize() {
        return ofMemorySize;
    }

    public void setOfMemorySize(Set<ConfigMemorySize> ofMemorySize) {
        this.ofMemorySize = ofMemorySize;
    }

    public Set<StringsConfig> getOfStringBean() {
        return ofStringBean;
    }

    public void setOfStringBean(Set<StringsConfig> ofStringBean) {
        this.ofStringBean = ofStringBean;
    }
}
