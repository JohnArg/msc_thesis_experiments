package jarg.thesis.experiments.benchmarks.hazelcast.client;

import com.hazelcast.core.IFunction;

/**
 * Just a dummy function used by client tests.
 */
public class DoublingFunction implements IFunction<Long, Long> {
    private static final long serialVersionUID = 1L;

    @Override
    public Long apply(Long number) {
        return number * 2;
    }
}

