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

package org.apache.seatunnel.common.utils;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;

public class SerializationUtils {

    public static String objectToString(Serializable obj) {
        if (obj != null) {
            return Base64.encodeBase64String(serialize(obj));
        }
        return null;
    }

    public static <T extends Serializable> T stringToObject(String str) {
        if (StringUtils.isNotEmpty(str)) {
            return deserialize(Base64.decodeBase64(str));
        }
        return null;
    }

    public static <T extends Serializable> byte[] serialize(T obj) {
        try (ByteArrayOutputStream b = new ByteArrayOutputStream(512);
                ObjectOutputStream out = new ObjectOutputStream(b)) {
            out.writeObject(obj);
            return b.toByteArray();
        } catch (final IOException ex) {
            throw new SerializationException(ex);
        }
    }

    public static <T extends Serializable> T deserialize(byte[] bytes) {
        try (ByteArrayInputStream s = new ByteArrayInputStream(bytes);
                ObjectInputStream in =
                        new ObjectInputStream(s) {
                            @Override
                            protected Class<?> resolveClass(ObjectStreamClass desc)
                                    throws IOException, ClassNotFoundException {
                                // make sure use current thread classloader
                                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                                if (cl == null) {
                                    return super.resolveClass(desc);
                                }
                                return Class.forName(desc.getName(), false, cl);
                            }
                        }) {
            @SuppressWarnings("unchecked")
            final T obj = (T) in.readObject();
            return obj;
        } catch (final ClassNotFoundException | IOException ex) {
            throw new SerializationException(ex);
        }
    }

    public static <T extends Serializable> T deserialize(byte[] bytes, ClassLoader classLoader) {
        try (ByteArrayInputStream s = new ByteArrayInputStream(bytes);
                ObjectInputStream in =
                        new ObjectInputStream(s) {
                            @Override
                            protected Class<?> resolveClass(ObjectStreamClass desc)
                                    throws IOException, ClassNotFoundException {
                                // make sure use current thread classloader
                                if (classLoader == null) {
                                    return super.resolveClass(desc);
                                }
                                return Class.forName(desc.getName(), false, classLoader);
                            }
                        }) {
            @SuppressWarnings("unchecked")
            final T obj = (T) in.readObject();
            return obj;
        } catch (final ClassNotFoundException | IOException ex) {
            throw new SerializationException(ex);
        }
    }
}
