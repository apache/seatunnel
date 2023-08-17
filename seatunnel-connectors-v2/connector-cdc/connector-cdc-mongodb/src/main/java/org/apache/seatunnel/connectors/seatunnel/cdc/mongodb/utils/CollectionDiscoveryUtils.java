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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils;

import org.apache.commons.collections4.CollectionUtils;

import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ADD_NS_FIELD_NAME;

public class CollectionDiscoveryUtils {

    public static final Bson ADD_NS_FIELD =
            BsonDocument.parse(
                    String.format(
                            "{'$addFields': {'%s': {'$concat': ['$ns.db', '.', '$ns.coll']}}}",
                            ADD_NS_FIELD_NAME));

    private CollectionDiscoveryUtils() {}

    public static @Nonnull List<String> databaseNames(
            @Nonnull MongoClient mongoClient, Predicate<String> databaseFilter) {
        List<String> databaseNames = new ArrayList<>();
        return mongoClient.listDatabaseNames().into(databaseNames).stream()
                .filter(databaseFilter)
                .collect(Collectors.toList());
    }

    public static @Nonnull List<String> collectionNames(
            MongoClient mongoClient,
            List<String> databaseNames,
            Predicate<String> collectionFilter) {
        return collectionNames(mongoClient, databaseNames, collectionFilter, String::toString);
    }

    public static <T> @Nonnull List<T> collectionNames(
            MongoClient mongoClient,
            @Nonnull List<String> databaseNames,
            Predicate<String> collectionFilter,
            Function<String, T> conversion) {
        List<T> collectionNames = new ArrayList<>();
        for (String dbName : databaseNames) {
            MongoDatabase db = mongoClient.getDatabase(dbName);
            StreamSupport.stream(db.listCollectionNames().spliterator(), false)
                    .map(collName -> dbName + "." + collName)
                    .filter(collectionFilter)
                    .map(conversion)
                    .forEach(collectionNames::add);
        }
        return collectionNames;
    }

    private static Predicate<String> stringListFilter(
            Predicate<String> filter, List<String> stringList) {
        if (CollectionUtils.isNotEmpty(stringList)) {
            List<Pattern> databasePatterns = includeListAsPatterns(stringList);
            filter = filter.and(anyMatch(databasePatterns));
        }
        return filter;
    }

    public static Predicate<String> databaseFilter(List<String> databaseList) {
        return stringListFilter(CollectionDiscoveryUtils::isNotBuiltInDatabase, databaseList);
    }

    public static Predicate<String> collectionsFilter(List<String> collectionList) {
        return stringListFilter(CollectionDiscoveryUtils::isNotBuiltInCollections, collectionList);
    }

    public static @Nonnull Predicate<String> anyMatch(List<Pattern> patterns) {
        return s -> patterns.stream().anyMatch(p -> p.matcher(s).matches());
    }

    public static Pattern includeListAsFlatPattern(List<String> includeList) {
        return includeListAsFlatPattern(includeList, CollectionDiscoveryUtils::completionPattern);
    }

    public static Pattern includeListAsFlatPattern(
            List<String> includeList, Function<String, Pattern> conversion) {
        if (includeList == null || includeList.isEmpty()) {
            return null;
        }
        String flatPatternLiteral =
                includeList.stream()
                        .map(conversion)
                        .map(Pattern::pattern)
                        .collect(Collectors.joining("|"));

        return Pattern.compile(flatPatternLiteral);
    }

    public static List<Pattern> includeListAsPatterns(List<String> includeList) {
        return includeListAsPatterns(includeList, CollectionDiscoveryUtils::completionPattern);
    }

    public static List<Pattern> includeListAsPatterns(
            List<String> includeList, Function<String, Pattern> conversion) {
        return includeList != null && !includeList.isEmpty()
                ? includeList.stream().map(conversion).collect(Collectors.toList())
                : Collections.emptyList();
    }

    public static boolean isNotBuiltInCollections(String fullName) {
        if (fullName == null) {
            return false;
        }
        MongoNamespace namespace = new MongoNamespace(fullName);
        return isNotBuiltInDatabase(namespace.getDatabaseName())
                && !namespace.getCollectionName().startsWith("system.");
    }

    public static boolean isNotBuiltInDatabase(String databaseName) {
        if (databaseName == null) {
            return false;
        }
        return !"local".equals(databaseName)
                && !"admin".equals(databaseName)
                && !"config".equals(databaseName);
    }

    public static @Nonnull Pattern completionPattern(@Nonnull String pattern) {
        if (pattern.startsWith("^") && pattern.endsWith("$")) {
            return Pattern.compile(pattern);
        }
        return Pattern.compile("^(" + pattern + ")$");
    }

    @Getter
    @AllArgsConstructor
    public static class CollectionDiscoveryInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<String> discoveredDatabases;

        private final List<String> discoveredCollections;
    }
}
