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

package io.github.interestinglab.waterdrop.config;

/**
 * The syntax of a character stream (<a href="http://json.org">JSON</a>, <a
 * href="https://github.com/lightbend/config/blob/master/HOCON.md">HOCON</a>
 * aka ".conf", or <a href=
 * "http://download.oracle.com/javase/7/docs/api/java/util/Properties.html#load%28java.io.Reader%29"
 * >Java properties</a>).
 * 
 */
public enum ConfigSyntax {
    /**
     * Pedantically strict <a href="http://json.org">JSON</a> format; no
     * comments, no unexpected commas, no duplicate keys in the same object.
     * Associated with the <code>.json</code> file extension and
     * <code>application/json</code> Content-Type.
     */
    JSON,
    /**
     * The JSON-superset <a
     * href="https://github.com/lightbend/config/blob/master/HOCON.md"
     * >HOCON</a> format. Associated with the <code>.conf</code> file extension
     * and <code>application/hocon</code> Content-Type.
     */
    CONF,
    /**
     * Standard <a href=
     * "http://download.oracle.com/javase/7/docs/api/java/util/Properties.html#load%28java.io.Reader%29"
     * >Java properties</a> format. Associated with the <code>.properties</code>
     * file extension and <code>text/x-java-properties</code> Content-Type.
     */
    PROPERTIES;
}
