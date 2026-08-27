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

package org.apache.seatunnel.api.configuration;

import java.util.Map;

/**
 * The interface that provides the ability to encrypt and decrypt {@link
 * org.apache.seatunnel.shade.com.typesafe.config.Config}
 */
public interface ConfigShade {

    /**
     * The unique identifier of the current interface, used it to select the correct {@link
     * ConfigShade}
     */
    String getIdentifier();

    /**
     * Encrypt the content
     *
     * @param content The content to encrypt
     */
    String encrypt(String content);

    /**
     * Decrypt the content
     *
     * @param content The content to decrypt
     */
    String decrypt(String content);

    /** To expand the options that user want to encrypt */
    default String[] sensitiveOptions() {
        return new String[0];
    }

    /**
     * this method will be called before the encrypt/decrpyt method. Users can use the props to
     * control the behavior of the encrypt/decrypt
     *
     * @param props the additional properties defined with the key `shade.props` in the
     *     configuration
     */
    default void open(Map<String, Object> props) {
        // default do nothing
    }
}
