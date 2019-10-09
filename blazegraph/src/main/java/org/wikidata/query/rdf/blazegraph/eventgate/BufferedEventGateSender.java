package org.wikidata.query.rdf.blazegraph.eventgate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferedEventGateSender implements EventGateSender {
    private final BlockingQueue<Event> queue;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public BufferedEventGateSender(int maxCap) {
        this.queue = new ArrayBlockingQueue<>(maxCap, true);
    }

    @Override
    public boolean push(Event event) {
        return offer(event);
    }

    @Override
    public boolean push(Collection<Event> events) {
        for (Event event : events) {
            if (!offer(event)) {
                return false;
            }
        }
        return true;
    }

    private boolean offer(Event event) {
        try {
            if (!queue.offer(event, 10, TimeUnit.MILLISECONDS)) {
                log.error("Cannot buffer event {}, queue full", event.getMetadata().getStream());
                return false;
            }
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public Resender newResender(EventGateSender sender, int bufferSize) {
        return new Resender(sender, bufferSize);
    }

    public final class Resender implements Runnable {
        private volatile boolean run = true;
        private final EventGateSender sender;
        private final int bufferSize;

        private Resender(EventGateSender sender, int bufferSize) {
            this.sender = sender;
            this.bufferSize = bufferSize;
        }

        public void stop() {
            this.run = false;
        }

        @Override
        public void run() {
            try {
                while (run || !queue.isEmpty()) {
                    Event event = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (event == null) {
                        continue;
                    }
                    List<Event> events = new ArrayList<>(bufferSize);
                    events.add(event);
                    queue.drainTo(events, bufferSize - 1);
                    this.sender.push(events);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
