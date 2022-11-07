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

package org.apache.seatunnel.api.configuration.util;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class OptionUtil {

    public static List<org.apache.seatunnel.api.configuration.Option<?>> getOptions(Class<?> clazz) throws InstantiationException, IllegalAccessException {
        Field[] fields = clazz.getDeclaredFields();
        List<org.apache.seatunnel.api.configuration.Option<?>> options = new ArrayList<>();
        Object object = clazz.newInstance();
        for (Field field : fields) {
            field.setAccessible(true);
            Option option = field.getAnnotation(Option.class);
            if (option != null) {
                options.add(new org.apache.seatunnel.api.configuration.Option<>(!StringUtils.isNotBlank(option.name()) ? formatUnderScoreCase(field.getName()) : option.name(),
                    new TypeReference<Object>() {
                        @Override
                        public Type getType() {
                            return field.getType();
                        }
                    }, field.get(object)).withDescription(option.description()));
            }
        }
        return options;
    }

    private static String formatUnderScoreCase(String camel) {
        StringBuilder underScore = new StringBuilder(String.valueOf(Character.toLowerCase(camel.charAt(0))));
        for (int i = 1; i < camel.length(); i++) {
            char c = camel.charAt(i);
            underScore.append(Character.isLowerCase(c) ? c : "_" + Character.toLowerCase(c));
        }
        return underScore.toString();
    }
}
