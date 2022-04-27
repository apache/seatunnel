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
        Assert.assertEquals("期望输出值和实际输出值不一致", output, new ScalarReplace("\\d+", "*", true, true).eval(input));

    }

    /**
     *   Replace the string that all regular expression matches
     *   Input : Tom's phone number is 123456789 , he's age is 24
     *   Output : Tom's phone number is * , he's age is *
     */
    @Test
    public void testExample02() {
        String input = "Tom's phone number is 123456789 , he's age is 24";
        String output = "Tom's phone number is * , he's age is *";
        Assert.assertEquals("期望输出值和实际输出值不一致", output, new ScalarReplace("\\d+", "*", true, false).eval(input));

    }

    /**
     *   Replace the string that all regular expression matches
     *   Input : Tom's phone number is 123456789 , he's age is 24
     *   Output : Tom is phone number is 123456789 , he is age is 24
     */
    @Test
    public void testExample03() {
        String input = "Tom's phone number is 123456789 , he's age is 24";
        String output = "Tom is phone number is 123456789 , he is age is 24";
        Assert.assertEquals("期望输出值和实际输出值不一致", output, new ScalarReplace("'s", " is", false, false).eval(input));

    }

}
