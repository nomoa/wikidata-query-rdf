package org.wikidata.query.rdf.blazegraph.eventgate;

import java.net.URI;
import java.time.Instant;
import java.util.function.Supplier;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"id", "dt", "request_id", "domain", "stream"})
public class EventMetadata {
    //private final String uri;
    private final String id;
    private final Instant dt;
    private final String requestId;
    private final String domain;
    private final String stream;

    public static EventMetadata fromHttpServletRequest(HttpServletRequest request, Supplier<String> uniqueIdGenerator, Instant now, String stream) {
        String reqId = request.getHeader("X-Request-Id");
        String id = uniqueIdGenerator.get();
        if (reqId == null) {
            reqId = uniqueIdGenerator.get();
        }
        URI uri = URI.create(request.getRequestURI());
        String domain = uri.getAuthority();
        return new EventMetadata(reqId, id, now, domain, stream);
    }

    public EventMetadata(String requestId, String id, Instant dt, String domain, String stream) {
        this.id = id;
        this.dt = dt;
        this.requestId = requestId;
        this.domain = domain;
        this.stream = stream;
    }

    public String getId() {
        return id;
    }

    @JsonProperty()
    public Instant getDt() {
        return dt;
    }

    @JsonProperty("request_id")
    public String getRequestId() {
        return requestId;
    }

    public String getDomain() {
        return domain;
    }

    public String getStream() {
        return stream;
    }
}
