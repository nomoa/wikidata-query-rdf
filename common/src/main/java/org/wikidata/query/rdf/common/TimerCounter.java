package org.wikidata.query.rdf.common;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import com.codahale.metrics.Counter;

/**
 * Simple wrapper around a Counter metric.
 * It allows to record times spent using the try-with-resource syntax:
 * <pre>
 * try (Context ctx : counter.time()) {
 *     // process to measure time of
 * }
 * </pre>
 */
public class TimerCounter {

    private final LongConsumer counter;
    private final TimeUnit unit;
    private final LongSupplier clock;

    public TimerCounter(LongConsumer counter, TimeUnit unit, LongSupplier clock) {
        this.counter = counter;
        this.unit = unit;
        this.clock = clock;
    }

    /**
     * Create a new TimerCounter reporting times as milliseconds on top of this Counter.
     */
    public static TimerCounter counter(Counter counter) {
        return new TimerCounter(counter::inc, TimeUnit.MILLISECONDS, System::nanoTime);
    }

    public Context time() {
        return new Context(counter, clock, unit);
    }

    public static class Context implements AutoCloseable {
        private final LongConsumer counter;
        private final LongSupplier clock;
        private final TimeUnit unit;
        private final long start;

        public Context(LongConsumer counter, LongSupplier clock, TimeUnit unit) {
            this.counter = counter;
            this.clock = clock;
            this.unit = unit;
            this.start = clock.getAsLong();
        }

        @Override
        public void close() {
            counter.accept(unit.convert(clock.getAsLong() - start, NANOSECONDS));
        }
    }
}
