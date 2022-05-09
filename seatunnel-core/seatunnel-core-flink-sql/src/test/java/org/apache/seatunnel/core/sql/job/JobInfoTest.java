package org.apache.seatunnel.core.sql.job;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class JobInfoTest {

    @Test
    public void testSubstitute() {
        String sql = "SELECT * FROM TABLE WHERE NAME = '${name}' AND DATE = '${date}'";
        JobInfo jobInfo = new JobInfo(sql);
        jobInfo.substitute(Arrays.asList("name=seatunnel", "date=2019-01-01"));
        Assert.assertEquals("SELECT * FROM TABLE WHERE NAME = 'seatunnel' AND DATE = '2019-01-01'", jobInfo.getJobContent());
    }
}
