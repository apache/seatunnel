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


import io.github.interestinglab.waterdrop.config.Optional;

public class ObjectsConfig {
    public static class ValueObject {
        @Optional
        private String optionalValue;
        private String mandatoryValue;

        public String getMandatoryValue() {
          return mandatoryValue;
        }

        public void setMandatoryValue(String mandatoryValue) {
          this.mandatoryValue = mandatoryValue;
        }

        public String getOptionalValue() {
          return optionalValue;
        }

        public void setOptionalValue(String optionalValue) {
          this.optionalValue = optionalValue;
        }
    }

    private ValueObject valueObject;

    public ValueObject getValueObject() {

        return valueObject;
    }

    public void setValueObject(ValueObject valueObject) {
        this.valueObject = valueObject;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ObjectsConfig)) {
            return false;
        }

        ObjectsConfig that = (ObjectsConfig) o;

        return !(getValueObject() != null ? !getValueObject().equals(that.getValueObject()) : that.getValueObject() != null);

    }

    @Override
    public int hashCode() {
        return getValueObject() != null ? getValueObject().hashCode() : 0;
    }


    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ObjectsConfig{");
        sb.append("innerType=").append(valueObject);
        sb.append('}');
        return sb.toString();
    }
}
