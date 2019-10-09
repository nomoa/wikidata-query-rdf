package org.wikidata.query.rdf.blazegraph.eventgate;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class EventGateHttpSender implements EventGateSender, AutoCloseable {
    private final CloseableHttpClient httpClient;
    private final String eventGateUri;
    private final ObjectMapper objectMapper;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public EventGateHttpSender(CloseableHttpClient httpClient, String eventGateUri) {
        this.httpClient = httpClient;
        this.eventGateUri = eventGateUri;
        this.objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    public static EventGateHttpSender build(String eventGateUri) {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(20);
        CloseableHttpClient httpclient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();
        return new EventGateHttpSender(httpclient, eventGateUri);
    }

    @Override
    public boolean push(Event query) {
        return push(Collections.singleton(query));
    }

    public boolean push(Collection<Event> events) {
        HttpPost post = new HttpPost(eventGateUri);
        try {
            post.setEntity(new StringEntity(objectMapper.writeValueAsString(events), ContentType.APPLICATION_JSON));
            try (CloseableHttpResponse response = httpClient.execute(post)) {
                if (response.getStatusLine().getStatusCode() >= 400) {
                    log.error("Cannot send events to eventgate endpoint: {}: {}", eventGateUri, response.getStatusLine());
                    return false;
                }
            }
            return true;
        } catch (IOException e) {
            log.error("Cannot send events to eventgate endpoint: {}", eventGateUri, e);
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
