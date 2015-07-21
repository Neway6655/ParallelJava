package com.neway6655.parallel.rx;

import org.junit.Test;

import java.util.concurrent.Callable;

public class ParallelExecutorTest {

    @Test
    public void testParallelExecute(){
        Callable<Object> callable1 = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return 1;
            }
        };

        Callable<Object> callable2 = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return 2;
            }
        };

        ParallelExecutor.parallelProcess(callable1, callable2);
    }

}
