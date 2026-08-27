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

package org.apache.seatunnel.engine.server.log;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;

public class LogLevelsTest {

    /** A logger name owned by this test only, so mutating its level cannot affect other tests. */
    private static final String TEST_LOGGER =
            "org.apache.seatunnel.engine.server.log.LogLevelsTest";

    private Level originalLevel;

    @AfterEach
    void restoreLevel() {
        if (originalLevel != null) {
            Configurator.setLevel(TEST_LOGGER, originalLevel);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"DEBUG", "debug", "Debug", " DEBUG ", "\tdebug\n"})
    void testLevelNameIsParsedCaseInsensitively(String name) {
        Assertions.assertEquals(Level.DEBUG, LogLevels.parse(name));
    }

    @ParameterizedTest
    @ValueSource(strings = {"DEBUGG", "verbose", "1", "INFO,DEBUG"})
    void testUnknownLevelNameIsRejected(String name) {
        // Level.getLevel returns null instead of throwing, so an unchecked value would reach
        // Configurator as a null level, which clears the level of the logger instead of leaving it
        // alone while the caller is told the request succeeded
        Assertions.assertNull(LogLevels.parse(name));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t"})
    void testBlankLevelNameIsRejected(String name) {
        Assertions.assertNull(LogLevels.parse(name));
    }

    @Test
    void testNullLevelNameIsRejected() {
        Assertions.assertNull(LogLevels.parse(null));
    }

    @Test
    void testValidNamesListsEveryStandardLevel() {
        String validNames = LogLevels.validNames();
        for (Level level : Level.values()) {
            Assertions.assertTrue(
                    validNames.contains(level.name()),
                    () -> "Level " + level.name() + " missing from: " + validNames);
        }
    }

    @Test
    void testValidNamesAreOrderedBySeverity() {
        // Level.values() reads a hash map, so without an explicit order the same rejected request
        // would list the valid levels differently between runs
        String validNames = LogLevels.validNames();
        List<Level> bySeverity =
                Arrays.asList(
                        Level.OFF,
                        Level.FATAL,
                        Level.ERROR,
                        Level.WARN,
                        Level.INFO,
                        Level.DEBUG,
                        Level.TRACE,
                        Level.ALL);
        int previousIndex = -1;
        for (Level level : bySeverity) {
            int index = validNames.indexOf(level.name());
            Assertions.assertTrue(
                    index > previousIndex,
                    () -> level.name() + " is out of severity order in: " + validNames);
            previousIndex = index;
        }
    }

    @Test
    void testApplyChangesTheEffectiveLevel() {
        originalLevel = LogManager.getLogger(TEST_LOGGER).getLevel();
        Level target = originalLevel == Level.TRACE ? Level.ERROR : Level.TRACE;

        LogLevels.apply(TEST_LOGGER, target);

        Assertions.assertEquals(target, LogManager.getLogger(TEST_LOGGER).getLevel());
    }
}
