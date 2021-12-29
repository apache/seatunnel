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

import org.apache.seatunnel.config.impl.ConfigBeanImpl;

/**
 * Factory for automatically creating a Java class from a {@link Config}.
 * See {@link #create(Config, Class)}.
 *
 * @since 1.3.0
 */
public class ConfigBeanFactory {

    /**
     * Creates an instance of a class, initializing its fields from a {@link Config}.
     * <p>
     * Example usage:
     *
     * <pre>
     * Config configSource = ConfigFactory.load().getConfig("foo");
     * FooConfig config = ConfigBeanFactory.create(configSource, FooConfig.class);
     * </pre>
     * <p>
     * The Java class should follow JavaBean conventions. Field types
     * can be any of the types you can normally get from a {@link
     * Config}, including <code>java.time.Duration</code> or {@link
     * ConfigMemorySize}. Fields may also be another JavaBean-style
     * class.
     * <p>
     * Fields are mapped to config by converting the config key to
     * camel case.  So the key <code>foo-bar</code> becomes JavaBean
     * setter <code>setFooBar</code>.
     *
     * @param config source of config information
     * @param clazz  class to be instantiated
     * @param <T>    the type of the class to be instantiated
     * @return an instance of the class populated with data from the config
     * @throws ConfigException.BadBean          If something is wrong with the JavaBean
     * @throws ConfigException.ValidationFailed If the config doesn't conform to the bean's implied schema
     * @throws ConfigException                  Can throw the same exceptions as the getters on <code>Config</code>
     * @since 1.3.0
     */
    public static <T> T create(Config config, Class<T> clazz) {
        return ConfigBeanImpl.createInternal(config, clazz);
    }
}
