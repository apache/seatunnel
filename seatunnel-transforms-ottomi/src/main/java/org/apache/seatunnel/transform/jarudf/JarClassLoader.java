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
package org.apache.seatunnel.transform.jarudf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * @author 徐正洲
 * @date 2024/6/21 14:12
 */
public class JarClassLoader extends ClassLoader implements Serializable {
    private final Map<String, byte[]> classBytes = new HashMap<>();

    public JarClassLoader(byte[] jarData) throws IOException {
        try (JarInputStream jarInputStream =
                new JarInputStream(new ByteArrayInputStream(jarData))) {
            JarEntry entry;
            while ((entry = jarInputStream.getNextJarEntry()) != null) {
                if (entry.getName().endsWith(".class")) {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    int data = jarInputStream.read();
                    while (data != -1) {
                        buffer.write(data);
                        data = jarInputStream.read();
                    }
                    byte[] classData = buffer.toByteArray();
                    String className = entry.getName().replace("/", ".").replace(".class", "");
                    classBytes.put(className, classData);
                }
            }
        }
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        byte[] bytes = classBytes.get(name);
        if (bytes == null) {
            throw new ClassNotFoundException(name);
        }
        return defineClass(name, bytes, 0, bytes.length);
    }
}
