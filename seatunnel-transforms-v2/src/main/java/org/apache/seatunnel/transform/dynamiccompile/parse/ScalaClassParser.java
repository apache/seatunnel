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
package org.apache.seatunnel.transform.dynamiccompile.parse;

import org.apache.seatunnel.shade.scala.tools.nsc.Settings;
import org.apache.seatunnel.shade.scala.tools.nsc.interpreter.IMain;
import org.apache.seatunnel.shade.scala.tools.nsc.interpreter.shell.ReplReporterImpl;

import org.apache.seatunnel.transform.exception.TransformException;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.seatunnel.transform.dynamiccompile.CompileTransformErrorCode.COMPILE_TRANSFORM_ERROR_CODE;

public class ScalaClassParser extends AbstractParser {

    private static final String SCALA_CLASS_NAME_PATTERN = "(?:class|object)\\s+(\\w+)";
    private static final Pattern CLASS_NAME_REGEX = Pattern.compile(SCALA_CLASS_NAME_PATTERN);
    private static IMain scalaInterpreter;

    static {
        try {
            Settings settings = new Settings();
            settings.usejavacp().v_$eq(true);
            scalaInterpreter = new IMain(settings, new ReplReporterImpl(settings));
        } catch (Exception e) {
            throw new TransformException(COMPILE_TRANSFORM_ERROR_CODE, e.getMessage());
        }
    }

    public static Class<?> parseSourceCodeWithCache(String sourceCode) {
        return classCache.computeIfAbsent(
                getClassKey(sourceCode),
                new Function<String, Class<?>>() {
                    @Override
                    public Class<?> apply(String classKey) {
                        String className = extractClassName(sourceCode);
                        return compileWithREPL(sourceCode, className);
                    }
                });
    }

    /** Extract class name from Scala source code */
    private static String extractClassName(String sourceCode) {
        Matcher matcher = CLASS_NAME_REGEX.matcher(sourceCode);
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new IllegalArgumentException("Cannot extract class name from Scala source code");
    }

    private static Class<?> compileWithREPL(String sourceCode, String className) {
        try {
            boolean compileResult = scalaInterpreter.compileString(sourceCode);
            if (!compileResult) {
                throw new RuntimeException("Scala REPL compilation failed");
            }
            ClassLoader replClassLoader = scalaInterpreter.classLoader();
            return replClassLoader.loadClass(className);
        } catch (Exception e) {
            throw new TransformException(COMPILE_TRANSFORM_ERROR_CODE, e.getMessage());
        }
    }
}
