package org.wikidata.query.rdf.blazegraph.eventgate;

import java.util.Map;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryEvent implements Event {
    private final EventMetadata metadata;
    private final EventHttpMetadata httpMetadata;
    @JsonProperty("backend_host")
    private final String backendHost;
    private final String namespace;
    private final String query;
    private final String format;
    private final Map<String, String> params;
    @JsonProperty("status_code")
    private final int statusCode;
    @JsonProperty("query_time")
    private final long queryTime;

    public QueryEvent(EventMetadata metadata, EventHttpMetadata http, String backendHost, String namespace, String query,
                      @Nullable String format, Map<String, String> params, int statusCode, long queryTime) {
        this.metadata = metadata;
        this.httpMetadata = http;
        this.backendHost = backendHost;
        this.namespace = namespace;
        this.query = query;
        this.format = format;
        this.params = params;
        this.statusCode = statusCode;
        this.queryTime = queryTime;
    }

    public static boolean acceptableQuery(HttpServletRequest request) {
        request.getParameterNames();
        return request.getParameter("query") != null;
    }

    public EventMetadata getMetadata() {
        return metadata;
    }

    public EventHttpMetadata getHttpMetadata() {
        return httpMetadata;
    }

    public String getBackendHost() {
        return backendHost;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getQuery() {
        return query;
    }

    public String getFormat() {
        return format;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public long getQueryTime() {
        return queryTime;
    }
}
