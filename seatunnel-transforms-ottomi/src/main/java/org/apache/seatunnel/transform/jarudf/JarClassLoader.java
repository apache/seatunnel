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
