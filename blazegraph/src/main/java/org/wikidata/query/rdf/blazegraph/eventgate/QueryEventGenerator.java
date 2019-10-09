package org.wikidata.query.rdf.blazegraph.eventgate;

import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class QueryEventGenerator {
    public static final Pattern NS_EXTRACTER = Pattern.compile("^/bigdata/namespace/([a-zA-Z0-9_]+)/sparql$");
    public static final String QUERY_PARAM = "query";
    private static final String FORMAT_PARAM = "format";
    private static final Set<String> EXCLUDED_PARAMS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(QUERY_PARAM, FORMAT_PARAM)));
    public static final String STREAM_NAME = "wdqs.sparqlquery";

    private final Supplier<String> uniqueIdGenerator;
    private final String hostname;

    public QueryEventGenerator(Supplier<String> uniqueIdGenerator, String hostname) {
        this.uniqueIdGenerator = uniqueIdGenerator;
        this.hostname = hostname;
    }

    public QueryEvent generateQueryEvent(HttpServletRequest request, HttpServletResponse response, long duration, Instant queryStartTime) {
        EventMetadata metadata = generateEventMetadata(request, queryStartTime);
        EventHttpMetadata httpMetadata = generateHttpMetadata(request);
        String format = request.getParameter("format");
        String query = request.getParameter("query");
        if (query == null) {
            throw new IllegalArgumentException("query parameter not found");
        }
        String namespace = extractBlazegraphNamespace(request.getRequestURI());
        if (namespace == null) {
            throw new IllegalArgumentException("namespace cannot be extracted from the request URI: " + request.getRequestURI());
        }

        return new QueryEvent(metadata, httpMetadata, hostname, namespace, query, format, extractRequestParams(request), response.getStatus(), duration);
    }

    private EventMetadata generateEventMetadata(HttpServletRequest request, Instant queryStartTime) {
        String reqId = request.getHeader("X-Request-Id");
        String id = uniqueIdGenerator.get();
        if (reqId == null) {
            reqId = uniqueIdGenerator.get();
        }
        URI uri = URI.create(request.getRequestURI());
        String domain = uri.getAuthority();
        return new EventMetadata(reqId, id, queryStartTime, domain, STREAM_NAME);
    }

    private EventHttpMetadata generateHttpMetadata(HttpServletRequest httpServletRequest) {
        Map<String, String> headers = Collections.list(httpServletRequest.getHeaderNames()).stream()
                .collect(Collectors.toMap(Function.identity(),
                        name -> String.join(",", Collections.list(httpServletRequest.getHeaders(name)))));
        // We run after the ClientIpFilter
        return new EventHttpMetadata(httpServletRequest.getMethod(), httpServletRequest.getRemoteAddr(),
                Collections.unmodifiableMap(headers), httpServletRequest.getCookies() != null);
    }

    private Map<String, String> extractRequestParams(HttpServletRequest httpServletRequest) {
        return Collections.list(httpServletRequest.getParameterNames()).stream()
                .filter(p -> !EXCLUDED_PARAMS.contains(p))
                .collect(Collectors.toMap(Function.identity(),
                        name -> String.join(",", httpServletRequest.getParameterValues(name))));
    }

    private String extractBlazegraphNamespace(String requestUri) {
        Matcher m = NS_EXTRACTER.matcher(requestUri);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }
}
