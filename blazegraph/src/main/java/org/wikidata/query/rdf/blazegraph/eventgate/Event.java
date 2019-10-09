package org.wikidata.query.rdf.blazegraph.eventgate;

import com.fasterxml.jackson.annotation.JsonProperty;

public interface Event {
    @JsonProperty("meta")
    EventMetadata getMetadata();
}
