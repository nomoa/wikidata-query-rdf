package org.wikidata.query.rdf.blazegraph.eventgate;

import java.util.Collection;
import java.util.Collections;

public interface EventGateSender {
    default boolean push(Event event) {
        return push(Collections.singleton(event));
    }
    boolean push(Collection<Event> events);
}
