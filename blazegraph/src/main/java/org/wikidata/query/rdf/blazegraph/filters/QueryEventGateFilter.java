package org.wikidata.query.rdf.blazegraph.filters;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.blazegraph.eventgate.BufferedEventGateSender;
import org.wikidata.query.rdf.blazegraph.eventgate.EventGateHttpSender;
import org.wikidata.query.rdf.blazegraph.eventgate.QueryEvent;
import org.wikidata.query.rdf.blazegraph.eventgate.QueryEventGenerator;

/**
 * This filter assumes that it is configured to only track
 * read queries.
 */
public class QueryEventGateFilter implements Filter {
    private ExecutorService executorService;
    private List<BufferedEventGateSender.Resender> resenders;
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private BufferedEventGateSender bufferedEventGateSender;
    private EventGateHttpSender httpSender;
    private QueryEventGenerator queryEenerator;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        int maxCap = 1000;
        int maxEventsPerHttpRequest = 10;
        int httpThreads = 2;
        String httpEndPoint = "https://todo.local/";
        String wdqsHostname = "localhost";
        executorService = Executors.newFixedThreadPool(2, (r) -> new Thread(r, this.getClass().getSimpleName()));
        bufferedEventGateSender = new BufferedEventGateSender(maxCap);
        httpSender = EventGateHttpSender.build(httpEndPoint);
        resenders = Stream.generate(() -> bufferedEventGateSender.newResender(httpSender, maxEventsPerHttpRequest))
                .limit(httpThreads)
                .collect(Collectors.toList());
        queryEenerator = new QueryEventGenerator(() -> UUID.randomUUID().toString(), wdqsHostname);
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
        HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;

        if (httpRequest.getParameter(QueryEventGenerator.QUERY_PARAM) == null || !QueryEventGenerator.NS_EXTRACTER.matcher(httpRequest.getQueryString()).matches()) {
            filterChain.doFilter(httpRequest, httpResponse);
        }

        if (queryEenerator.generateQueryEvent() == null) {
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }
    }

    @Override
    public void destroy() {
        resenders.forEach(BufferedEventGateSender.Resender::stop);
        executorService.shutdown();
        boolean succeed = false;
        try {
             succeed = executorService.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (!succeed) {
            log.error("Failed to wait for all queryevents to be drained");
        }
        try {
            httpSender.close();
        } catch (IOException e) {
            log.error("Error while closing EventGate http client", e);
        }
    }
}
