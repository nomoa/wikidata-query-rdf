package org.wikidata.query.rdf.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class TimerCounterUnitTest {
    @Test
    @SuppressWarnings("EmptyBlock")
    public void test() {
        Iterator<Long> clockTimes = Arrays.asList(0L, TimeUnit.MILLISECONDS.toNanos(150)).iterator();
        List<Long> recordTimes = new ArrayList<>();
        TimerCounter counter = new TimerCounter(recordTimes::add, TimeUnit.MILLISECONDS, clockTimes::next);
        try (TimerCounter.Context ctx = counter.time()) {
        }
        assertThat(recordTimes).containsExactly(150L);
    }
}
