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

import org.apache.seatunnel.shade.org.codehaus.commons.compiler.CompileException;
import org.apache.seatunnel.shade.org.codehaus.janino.ClassBodyEvaluator;

import java.util.function.Function;

public class JavaClassParser extends AbstractParser {

    public static Class<?> parseSourceCodeWithCache(String sourceCode) {
        return classCache.computeIfAbsent(
                getClassKey(sourceCode),
                new Function<String, Class<?>>() {
                    @Override
                    public Class<?> apply(String classKey) {
                        return getInnerClass(sourceCode);
                    }
                });
    }

    private static Class<?> getInnerClass(String FilePathOrSourceCode) {
        try {
            ClassBodyEvaluator cbe = new ClassBodyEvaluator();

            cbe.cook(FilePathOrSourceCode);

            return cbe.getClazz();

        } catch (CompileException e) {
            throw new RuntimeException(e);
        }
    }
}
