package org.wikidata.query.rdf.blazegraph.eventgate;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BufferedEventGateSenderUnitTest {
    @Captor
    private ArgumentCaptor<Collection<Event>> captor;

    @Before
    public void init(){
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testPushSingle() {
        BufferedEventGateSender bufferedEventGateSender = new BufferedEventGateSender(1);
        Event e = EventGateTestUtils.newTestEvent();
        assertThat(bufferedEventGateSender.push(e)).isTrue();
        EventGateSender resendGate = mock(EventGateSender.class);

        BufferedEventGateSender.Resender resender = bufferedEventGateSender.newResender(resendGate, 1);
        // Stop before so that we drain the queue without looping for ever
        resender.stop();
        resender.run();
        verify(resendGate, times(1)).push(captor.capture());
        assertThat(captor.getValue()).containsExactly(e);
    }

    @Test
    public void testPushReturnsFalseWhenFull() {
        BufferedEventGateSender bufferedEventGateSender = new BufferedEventGateSender(1);
        Event e = EventGateTestUtils.newTestEvent();
        assertThat(bufferedEventGateSender.push(e)).isTrue();
        assertThat(bufferedEventGateSender.push(e)).isFalse();
        assertThat(bufferedEventGateSender.push(Collections.singleton(e))).isFalse();
    }

    @Test
    public void testBufferingOnResend() {
        BufferedEventGateSender bufferedEventGateSender = new BufferedEventGateSender(10);
        List<Event> events = Stream.generate(EventGateTestUtils::newTestEvent).limit(10).collect(toList());
        Event[] batch1 = events.subList(0, 5).toArray(new Event[0]);
        Event[] batch2 = events.subList(5, 10).toArray(new Event[0]);
        assertThat(bufferedEventGateSender.push(events)).isTrue();
        EventGateSender resendGate = mock(EventGateSender.class);

        BufferedEventGateSender.Resender resender = bufferedEventGateSender.newResender(resendGate, 5);
        // Stop before so that we drain the queue without looping for ever
        resender.stop();
        resender.run();
        verify(resendGate, times(2)).push(captor.capture());
        assertThat(captor.getAllValues()).size().isEqualTo(2);

        assertThat(captor.getAllValues().get(0)).containsExactly(batch1).size().isEqualTo(5);
        assertThat(captor.getAllValues().get(1)).containsExactly(batch2).size().isEqualTo(5);
    }

    @Test(timeout = 3000L)
    public void testConcurrency() throws InterruptedException {
        // More of an integration test to test that the events are not leaked when closing the resenders.
        AtomicInteger threadIDs = new AtomicInteger();
        // forbidden apis does not want to use the default thread factory
        ExecutorService service = Executors.newFixedThreadPool(5, (r) -> new Thread(r, this.getClass().getName() + threadIDs.getAndIncrement()));
        BufferedEventGateSender bufferedEventGateSender = new BufferedEventGateSender(10);
        List<Event> events1 = Collections.synchronizedList(new ArrayList<>());
        List<Event> events2 = Collections.synchronizedList(new ArrayList<>());
        BufferedEventGateSender.Resender resender1 = bufferedEventGateSender.newResender(events1::addAll, 5);
        BufferedEventGateSender.Resender resender2 = bufferedEventGateSender.newResender(events2::addAll, 5);
        service.submit(resender1);
        service.submit(resender2);
        int nbEvents = 0;

        // Wait a bit so that we test that the resender loop does
        // properly handle the case when poll returns false (empty queue).
        // Ensuring this with a sleep is a bit weak but making this assertion properly would
        // require to expose more states or to mock the internal BlockingQueue.
        Thread.sleep(1000);
        Random r = new Random(123);
        Supplier<Collection<Event>> batchGenerator = () -> Stream.generate(EventGateTestUtils::newTestEvent)
                .limit(r.nextInt(5))
                .collect(Collectors.toList());

        for (int i = 0; i < 10; i++) {
            Collection<Event> events = batchGenerator.get();
            nbEvents += events.size();

            assertThat(bufferedEventGateSender.push(events)).isTrue();
            while ((events1.size() + events2.size()) < nbEvents) {
                Thread.sleep(10);
            }
        }
        Collection<Event> events = batchGenerator.get();
        nbEvents += events.size();
        assertThat(bufferedEventGateSender.push(events)).isTrue();
        resender1.stop();
        resender2.stop();
        service.shutdown();
        service.awaitTermination(1, TimeUnit.SECONDS);
        assertThat(events1.size() + events2.size()).isEqualTo(nbEvents);
    }

    @Test(timeout = 1000L)
    public void testPushInterruption() throws InterruptedException {
        BufferedEventGateSender bufferedEventGateSender = new BufferedEventGateSender(1);
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicBoolean isInterrupted = new AtomicBoolean(false);
        AtomicBoolean pushed = new AtomicBoolean(false);

        // Use a new thread to not mess up with JUnit threads
        Thread t = new Thread(() -> {
            Thread.currentThread().interrupt();
            pushed.set(bufferedEventGateSender.push(EventGateTestUtils.newTestEvent()));
            isInterrupted.set(Thread.currentThread().isInterrupted());
            done.set(true);
        });
        t.start();
        while (!done.get()) {
            Thread.sleep(10);
        }
        assertThat(pushed.get()).isFalse();
        assertThat(isInterrupted.get()).isTrue();
    }

    @Test(timeout = 1000L)
    public void testResendInterruption() throws InterruptedException {
        BufferedEventGateSender bufferedEventGateSender = new BufferedEventGateSender(1);
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicBoolean isInterrupted = new AtomicBoolean(false);

        BufferedEventGateSender.Resender resender = bufferedEventGateSender.newResender(mock(EventGateSender.class), 10);
        Runnable r = () -> {
            Thread.currentThread().interrupt();
            resender.run();
            // should immediately return
            isInterrupted.set(Thread.currentThread().isInterrupted());
            done.set(true);
        };
        Thread t = new Thread(r);
        t.start();
        int maxWait = 10;
        while (!done.get() && maxWait-- > 0) {
            Thread.sleep(10);
        }
        assertThat(done.get()).isTrue();
        assertThat(isInterrupted.get()).isTrue();
    }
}