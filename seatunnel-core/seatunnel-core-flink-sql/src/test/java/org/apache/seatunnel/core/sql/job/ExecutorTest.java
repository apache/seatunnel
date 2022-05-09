package org.apache.seatunnel.core.sql.job;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class ExecutorTest {

    @Test
    public void testSetOperationParse() {
        String stmt = "SET parallelism.default = 1";
        Optional<String[]> ops = Executor.parseSetOperation(stmt);
        Assert.assertTrue(ops.isPresent());
        Assert.assertEquals(2, ops.get().length);
        Assert.assertEquals("parallelism.default", ops.get()[0]);
        Assert.assertEquals("1", ops.get()[1]);

        stmt = "SET parallelism.default";
        ops = Executor.parseSetOperation(stmt);
        Assert.assertFalse(ops.isPresent());

        stmt = "CREATE TABLE IF NOT EXISTS test (id INT, name VARCHAR)";
        ops = Executor.parseSetOperation(stmt);
        Assert.assertFalse(ops.isPresent());
    }
}
