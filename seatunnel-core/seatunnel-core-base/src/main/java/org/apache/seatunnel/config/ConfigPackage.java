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

package org.apache.seatunnel.config;

public class ConfigPackage {

    private final String packagePrefix;
    private final String upperEngine;
    private final String sourcePackage;
    private final String transformPackage;
    private final String sinkPackage;
    private final String envPackage;
    private final String baseSourceClass;
    private final String baseTransformClass;
    private final String baseSinkClass;

    public ConfigPackage(String engine) {
        this.packagePrefix = "org.apache.seatunnel." + engine;
        this.upperEngine = engine.substring(0, 1).toUpperCase() + engine.substring(1);
        this.sourcePackage = packagePrefix + ".source";
        this.transformPackage = packagePrefix + ".transform";
        this.sinkPackage = packagePrefix + ".sink";
        this.envPackage = packagePrefix + ".env";
        this.baseSourceClass = packagePrefix + ".Base" + upperEngine + "Source";
        this.baseTransformClass = packagePrefix + ".Base" + upperEngine + "Transform";
        this.baseSinkClass = packagePrefix + ".Base" + upperEngine + "Sink";
    }

    public String getPackagePrefix() {
        return packagePrefix;
    }

    public String getUpperEngine() {
        return upperEngine;
    }

    public String getSourcePackage() {
        return sourcePackage;
    }

    public String getTransformPackage() {
        return transformPackage;
    }

    public String getSinkPackage() {
        return sinkPackage;
    }

    public String getEnvPackage() {
        return envPackage;
    }

    public String getBaseSourceClass() {
        return baseSourceClass;
    }

    public String getBaseTransformClass() {
        return baseTransformClass;
    }

    public String getBaseSinkClass() {
        return baseSinkClass;
    }
}
