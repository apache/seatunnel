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
package org.apache.seatunnel.flink.transform;


import junit.framework.Assert;
import org.junit.Test;

public class TestReplace {

    /**
     * Replace the string that the first regular expression matches
     * Input : Tom's phone number is 123456789 , he's age is 24
     * Output : Tom's phone number is * , he's age is 24
     */
    @Test
    public void testExample01() {
        String input = "Tom's phone number is 123456789 , he's age is 24";
        String output = "Tom's phone number is * , he's age is 24";
        Assert.assertEquals("The expected output is inconsistent with the actual output", output, new ScalarReplace("\\d+", "*", true, true).eval(input));

    }

    /**
     * Replace the string that all regular expression matches
     * Input : Tom's phone number is 123456789 , he's age is 24
     * Output : Tom's phone number is * , he's age is *
     */
    @Test
    public void testExample02() {
        String input = "Tom's phone number is 123456789 , he's age is 24";
        String output = "Tom's phone number is * , he's age is *";
        Assert.assertEquals("The expected output is inconsistent with the actual output", output, new ScalarReplace("\\d+", "*", true, false).eval(input));

    }

    /**
     * Replace the string that all regular expression matches
     * Input : Tom's phone number is 123456789 , he's age is 24
     * Output : Tom is phone number is 123456789 , he is age is 24
     */
    @Test
    public void testExample03() {
        String input = "Tom's phone number is 123456789 , he's age is 24";
        String output = "Tom is phone number is 123456789 , he is age is 24";
        Assert.assertEquals("The expected output is inconsistent with the actual output", output, new ScalarReplace("'s", " is", false, false).eval(input));

    }

}
