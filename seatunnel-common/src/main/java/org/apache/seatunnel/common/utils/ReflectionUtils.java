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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

public class ReflectionUtils {

    public static Optional<Method> getDeclaredMethod(
            Class<?> clazz, String methodName, Class<?>... parameterTypes) {

        Optional<Method> method = Optional.empty();
        Method m;
        for (; clazz != null; clazz = clazz.getSuperclass()) {
            try {
                m = clazz.getDeclaredMethod(methodName, parameterTypes);
                m.setAccessible(true);
                return Optional.of(m);
            } catch (NoSuchMethodException e) {
                // do nothing
            }
        }

        return method;
    }

    public static Optional<Object> getField(Object object, Class<?> clazz, String fieldName) {
        try {
            Class<?> searchType = clazz;
            while (!Object.class.equals(searchType) && searchType != null) {
                Field[] fields = searchType.getDeclaredFields();
                for (Field field : fields) {
                    if (fieldName.equals(field.getName())) {
                        field.setAccessible(true);
                        return Optional.of(field.get(object));
                    }
                }
                // find super class
                searchType = searchType.getSuperclass();
            }
            return Optional.empty();
        } catch (IllegalAccessException | IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    public static Optional<Object> getField(Object object, String fieldName) {
        return getField(object, object.getClass(), fieldName);
    }

    public static void setField(Object object, Class<?> clazz, String fieldName, Object value) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("field set failed", e);
        }
    }

    public static void setField(Object object, String fieldName, Object value) {
        setField(object, object.getClass(), fieldName, value);
    }

    public static Object invoke(Object object, String methodName, Object... args) {
        Class<?>[] argTypes = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argTypes[i] = args[i].getClass();
        }
        return invoke(object, methodName, argTypes, args);
    }

    public static Object invoke(
            Object object, String methodName, Class<?>[] argTypes, Object[] args) {
        try {
            Optional<Method> method = getDeclaredMethod(object.getClass(), methodName, argTypes);
            if (method.isPresent()) {
                method.get().setAccessible(true);
                return method.get().invoke(object, args);
            } else {
                throw new NoSuchMethodException(
                        String.format(
                                "method invoke failed, no such method '%s' in '%s'",
                                methodName, object.getClass()));
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("method invoke failed", e);
        }
    }
}
