package org.wikidata.query.rdf.blazegraph.eventgate;

import java.util.Map;

public class EventHttpMetadata {
    private final String method;
    private final String clientIp;
    private final Map<String, String> requestHeaders;
    private final boolean hasCookies;

    public EventHttpMetadata(String method, String clientIp, Map<String, String> requestHeaders, boolean hasCookies) {
        this.method = method;
        this.clientIp = clientIp;
        this.requestHeaders = requestHeaders;
        this.hasCookies = hasCookies;
    }
}
