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

package org.apache.seatunnel.config.beanconfig;

public class StringsConfig {
    String abcd;
    String yes;

    public String getAbcd() {
        return abcd;
    }

    public void setAbcd(String s) {
        abcd = s;
    }

    public String getYes() {
        return yes;
    }

    public void setYes(String s) {
        yes = s;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof StringsConfig) {
            StringsConfig sc = (StringsConfig) o;
            return sc.abcd.equals(abcd) &&
                    sc.yes.equals(yes);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int h = 41 * (41 + abcd.hashCode());
        return h + yes.hashCode();
    }

    @Override
    public String toString() {
        return "StringsConfig(" + abcd + "," + yes + ")";
    }
}
